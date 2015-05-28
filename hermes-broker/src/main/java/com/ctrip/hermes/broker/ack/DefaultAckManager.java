package com.ctrip.hermes.broker.ack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.ack.DefaultAckManager.Operation.Type;
import com.ctrip.hermes.broker.ack.internal.AckHolder;
import com.ctrip.hermes.broker.ack.internal.BatchResult;
import com.ctrip.hermes.broker.ack.internal.ContinuousRange;
import com.ctrip.hermes.broker.ack.internal.DefaultAckHolder;
import com.ctrip.hermes.broker.ack.internal.EnumRange;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.AckMessageCommand.AckContext;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = AckManager.class)
public class DefaultAckManager implements AckManager, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultAckManager.class);

	// TODO while consumer disconnect, clear holder and offset
	private ConcurrentMap<Triple<Tpp, String, Boolean>, AckHolder<Integer>> m_holders = new ConcurrentHashMap<>();

	private BlockingQueue<Operation> m_opQueue;

	private ScheduledExecutorService m_scheduledExecutorService;

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private MetaService m_metaService;

	@Inject
	private BrokerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Override
	public void initialize() throws InitializationException {
		m_opQueue = new LinkedBlockingQueue<>(m_config.getAckManagerOpQueueSize());

		m_scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      "AckManagerWorker", false));
		m_scheduledExecutorService.scheduleWithFixedDelay(new AckTask(), 0, m_config.getAckManagerCheckIntervalMillis(),
		      TimeUnit.MILLISECONDS);

	}

	@Override
	public void delivered(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqsAndRemainingRetries) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		m_opQueue.offer(new Operation(key, Type.DELIVERED, msgSeqsAndRemainingRetries, m_systemClockService.now()));
	}

	private void ensureMapEntryExist(Triple<Tpp, String, Boolean> key) {
		if (!m_holders.containsKey(key)) {
			m_holders.putIfAbsent(
			      key,
			      new DefaultAckHolder<Integer>(m_metaService.getAckTimeoutSecondsTopicAndConsumerGroup(key.getFirst()
			            .getTopic(), key.getMiddle()) * 1000));
		}
	}

	@Override
	public void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		for (AckContext context : ackContexts) {
			m_opQueue.offer(new Operation(key, Type.ACK, context.getMsgSeq(), m_systemClockService.now()));
		}
	}

	@Override
	public void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		ensureMapEntryExist(key);
		for (AckContext context : nackContexts) {
			m_opQueue.offer(new Operation(key, Type.NACK, context.getMsgSeq(), m_systemClockService.now()));
		}
	}

	private class AckTask implements Runnable {
		private List<Operation> m_todos = new ArrayList<Operation>();

		@Override
		public void run() {
			try {
				handleOperations();

				checkHolders();
			} catch (Exception e) {
				log.error("Exception occured while executing ack task.", e);
			}
		}

		@SuppressWarnings("unchecked")
		private void handleOperations() {
			try {
				if (m_todos.isEmpty()) {
					m_opQueue.drainTo(m_todos, m_config.getAckManagerOpHandlingBatchSize());
				}

				if (m_todos.isEmpty()) {
					return;
				}

				for (Operation op : m_todos) {
					switch (op.getType()) {
					case ACK:
						m_holders.get(op.getKey()).acked((Long) op.getData(), true);
						break;
					case NACK:
						m_holders.get(op.getKey()).acked((Long) op.getData(), false);
						break;
					case DELIVERED:
						m_holders.get(op.getKey()).delivered((List<Pair<Long, Integer>>) op.getData(), op.getCreateTime());
						break;

					default:
						break;
					}
				}

				m_todos.clear();
			} catch (Exception e) {
				log.error("Exception occured while handling operations.", e);
			}
		}

		private void checkHolders() {
			for (Map.Entry<Triple<Tpp, String, Boolean>, AckHolder<Integer>> entry : m_holders.entrySet()) {
				AckHolder<Integer> holder = entry.getValue();
				Tpp tpp = entry.getKey().getFirst();
				String groupId = entry.getKey().getMiddle();
				boolean isResend = entry.getKey().getLast();

				BatchResult<Integer> result = holder.scan();

				if (result != null) {
					ackOrNackMessageQueue(tpp, groupId, isResend, result);
				}
			}
		}

		private void ackOrNackMessageQueue(Tpp tpp, String groupId, boolean isResend, BatchResult<Integer> result) {
			ContinuousRange doneRange = result.getDoneRange();
			EnumRange<Integer> failRange = result.getFailRange();
			if (failRange != null) {
				if (log.isDebugEnabled()) {
					log.debug(
					      "Nack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, msgIdToRemainingRetries={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, failRange.getOffsets());
				}

				try {
					m_queueManager.nack(tpp, groupId, isResend, failRange.getOffsets());
				} catch (Exception e) {
					log.error(
					      String.format(
					            "Failed to nack messages(topic=%s, partition=%s, priority=%s, groupId=%s, isResend=%s, msgIdToRemainingRetries=%s).",
					            tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend,
					            failRange.getOffsets()), e);
				}
			}

			if (doneRange != null) {
				if (log.isDebugEnabled()) {
					log.debug("Ack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, endOffset={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, doneRange.getEnd());
				}
				try {
					m_queueManager.ack(tpp, groupId, isResend, doneRange.getEnd());
				} catch (Exception e) {
					log.error(String.format(
					      "Ack messages(topic=%s, partition=%s, priority=%s, groupId=%s, isResend=%s, endOffset=%s).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, doneRange.getEnd()), e);
				}
			}
		}
	}

	static class Operation {
		public enum Type {
			ACK, NACK, DELIVERED;
		}

		private Triple<Tpp, String, Boolean> m_key;

		private Object m_data;

		private Type m_type;

		private long m_createTime;

		Operation(Triple<Tpp, String, Boolean> key, Type type, Object data, long createTime) {
			m_key = key;
			m_data = data;
			m_type = type;
			m_createTime = createTime;
		}

		public Triple<Tpp, String, Boolean> getKey() {
			return m_key;
		}

		public Object getData() {
			return m_data;
		}

		public Type getType() {
			return m_type;
		}

		public long getCreateTime() {
			return m_createTime;
		}

	}
}
