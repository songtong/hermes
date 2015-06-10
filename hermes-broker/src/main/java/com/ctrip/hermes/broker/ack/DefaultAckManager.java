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

import com.ctrip.hermes.broker.ack.DefaultAckManager.Operation.Type;
import com.ctrip.hermes.broker.ack.internal.AckHolder;
import com.ctrip.hermes.broker.ack.internal.BatchResult;
import com.ctrip.hermes.broker.ack.internal.ContinuousRange;
import com.ctrip.hermes.broker.ack.internal.DefaultAckHolder;
import com.ctrip.hermes.broker.ack.internal.EnumRange;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
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
	private ConcurrentMap<Pair<Tpp, String>, AckHolder<MessageMeta>> m_holders = new ConcurrentHashMap<>();

	private ConcurrentMap<Pair<Tpp, String>, AckHolder<MessageMeta>> m_resendHolders = new ConcurrentHashMap<>();

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
	public void delivered(Tpp tpp, String groupId, boolean resend, List<MessageMeta> msgMetas) {
		resetTppIfResend(tpp, resend);

		Pair<Tpp, String> key = new Pair<>(tpp, groupId);
		ensureMapEntryExist(key, resend);

		List<Pair<Long, MessageMeta>> msgId2Metas = new ArrayList<>(msgMetas.size());
		for (MessageMeta msgMeta : msgMetas) {
			msgId2Metas.add(new Pair<>(msgMeta.getId(), msgMeta));
		}

		boolean offered = m_opQueue.offer(new Operation(key, resend, Type.DELIVERED, msgId2Metas, m_systemClockService
		      .now()));
		logOfferFail("delivered", offered);
	}

	private void logOfferFail(String type, boolean offered) {
		if (!offered) {
			log.warn("Operation queue full when doing {}", type);
		}
	}

	private void resetTppIfResend(Tpp tpp, boolean resend) {
		if (resend) {
			tpp.setPriority(false);
		}
	}

	private void ensureMapEntryExist(Pair<Tpp, String> key, boolean isResend) {
		ConcurrentMap<Pair<Tpp, String>, AckHolder<MessageMeta>> holders = getHolders(isResend);

		if (!holders.containsKey(key)) {
			int timeout = m_metaService.getAckTimeoutSecondsTopicAndConsumerGroup(key.getKey().getTopic(), key.getValue()) * 1000;
			DefaultAckHolder<MessageMeta> newHolder = new DefaultAckHolder<MessageMeta>(timeout);
			holders.putIfAbsent(key, newHolder);
		}

	}

	@Override
	public void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts) {
		resetTppIfResend(tpp, resend);
		Pair<Tpp, String> key = new Pair<>(tpp, groupId);
		ensureMapEntryExist(key, resend);
		for (AckContext context : ackContexts) {
			boolean offered = m_opQueue.offer(new Operation(key, resend, Type.ACK, context.getMsgSeq(),
			      m_systemClockService.now()));
			logOfferFail("acked", offered);
		}
	}

	@Override
	public void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts) {
		resetTppIfResend(tpp, resend);
		Pair<Tpp, String> key = new Pair<>(tpp, groupId);
		ensureMapEntryExist(key, resend);
		for (AckContext context : nackContexts) {
			boolean offered = m_opQueue.offer(new Operation(key, resend, Type.NACK, context.getMsgSeq(),
			      m_systemClockService.now()));
			logOfferFail("nacked", offered);
		}
	}

	private ConcurrentMap<Pair<Tpp, String>, AckHolder<MessageMeta>> getHolders(boolean isResend) {
		return isResend ? m_resendHolders : m_holders;
	}

	private class AckTask implements Runnable {
		private List<Operation> m_todos = new ArrayList<Operation>();

		@Override
		public void run() {
			try {
				handleOperations();
				checkHolders(false);
				checkHolders(true);
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
					ConcurrentMap<Pair<Tpp, String>, AckHolder<MessageMeta>> holder = getHolders(op.isResend());

					switch (op.getType()) {
					case ACK:
						holder.get(op.getKey()).acked((Long) op.getData(), true);
						break;
					case NACK:
						holder.get(op.getKey()).acked((Long) op.getData(), false);
						break;
					case DELIVERED:
						holder.get(op.getKey()).delivered((List<Pair<Long, MessageMeta>>) op.getData(), op.getCreateTime());
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

		private void checkHolders(boolean isResend) {
			ConcurrentMap<Pair<Tpp, String>, AckHolder<MessageMeta>> holders = getHolders(isResend);

			for (Map.Entry<Pair<Tpp, String>, AckHolder<MessageMeta>> entry : holders.entrySet()) {
				AckHolder<MessageMeta> holder = entry.getValue();
				Tpp tpp = entry.getKey().getKey();
				String groupId = entry.getKey().getValue();

				BatchResult<MessageMeta> result = holder.scan();

				if (result != null) {
					ackOrNackMessageQueue(tpp, groupId, isResend, result);
				}
			}
		}

		private void ackOrNackMessageQueue(Tpp tpp, String groupId, boolean isResend, BatchResult<MessageMeta> result) {
			ContinuousRange doneRange = result.getDoneRange();
			EnumRange<MessageMeta> failRange = result.getFailRange();
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

		private Pair<Tpp, String> m_key;

		private boolean m_resend;

		private Object m_data;

		private Type m_type;

		private long m_createTime;

		Operation(Pair<Tpp, String> key, boolean isResend, Type type, Object data, long createTime) {
			m_key = key;
			m_resend = isResend;
			m_data = data;
			m_type = type;
			m_createTime = createTime;
		}

		public boolean isResend() {
			return m_resend;
		}

		public Pair<Tpp, String> getKey() {
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
