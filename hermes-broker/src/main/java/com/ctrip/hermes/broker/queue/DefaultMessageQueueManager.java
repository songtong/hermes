package com.ctrip.hermes.broker.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation.Type;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.AckMessageCommand.AckContext;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.google.common.util.concurrent.ListenableFuture;

@Named(type = MessageQueueManager.class)
public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueManager.class);

	@Inject
	private MessageQueuePartitionFactory m_queueFactory;

	@Inject
	private BrokerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	// one <topic, partition, lease> mapping to one MessageQueue
	private Map<Pair<String, Integer>, MessageQueue> m_messageQueues = new ConcurrentHashMap<>();

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	private ScheduledExecutorService m_ackOpExecutor;

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageBatchWithRawData data, Lease lease) {
		if (!m_stopped.get()) {
			return getMessageQueue(tpp.getTopic(), tpp.getPartition()).appendMessageAsync(tpp.isPriority(), data, lease);
		} else {
			return null;
		}
	}

	@Override
	public MessageQueueCursor getCursor(Tpg tpg, Lease lease) {
		if (!m_stopped.get()) {
			return getMessageQueue(tpg.getTopic(), tpg.getPartition()).getCursor(tpg.getGroupId(), lease);
		} else {
			return null;
		}
	}

	private MessageQueue getMessageQueue(String topic, int partition) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		if (!m_messageQueues.containsKey(key)) {
			synchronized (m_messageQueues) {
				if (!m_messageQueues.containsKey(key)) {
					MessageQueue mqp = m_queueFactory.getMessageQueue(topic, partition, m_ackOpExecutor);
					m_messageQueues.put(key, mqp);
				}
			}
		}

		return m_messageQueues.get(key);
	}

	@Override
	public void stop() {
		m_ackOpExecutor.shutdown();
		while (!m_ackOpExecutor.isTerminated()) {
			try {
				m_ackOpExecutor.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// ignore
			}
		}

		for (MessageQueue mq : m_messageQueues.values()) {
			mq.stop();
		}
	}

	@Override
	public void delivered(Tpp tpp, String groupId, boolean resend, List<MessageMeta> msgMetas) {
		if (m_stopped.get()) {
			return;
		}

		resetPriorityIfResend(tpp, resend);
		Pair<Boolean, String> key = new Pair<>(tpp.isPriority(), groupId);
		List<Pair<Long, MessageMeta>> msgId2Metas = new ArrayList<>(msgMetas.size());
		for (MessageMeta msgMeta : msgMetas) {
			msgId2Metas.add(new Pair<>(msgMeta.getId(), msgMeta));
		}

		boolean offered = getMessageQueue(tpp.getTopic(), tpp.getPartition()).offer(
		      new Operation(key, resend, Type.DELIVERED, msgId2Metas, m_systemClockService.now()));
		logIfOfferFail("delivered", offered);
	}

	@Override
	public void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts) {
		if (m_stopped.get()) {
			return;
		}
		resetPriorityIfResend(tpp, resend);
		Pair<Boolean, String> key = new Pair<>(tpp.isPriority(), groupId);
		for (AckContext context : ackContexts) {
			boolean offered = getMessageQueue(tpp.getTopic(), tpp.getPartition()).offer(
			      new Operation(key, resend, Type.ACK, context.getMsgSeq(), m_systemClockService.now()));
			logIfOfferFail("acked", offered);
		}
	}

	@Override
	public void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts) {
		if (m_stopped.get()) {
			return;
		}
		resetPriorityIfResend(tpp, resend);
		Pair<Boolean, String> key = new Pair<>(tpp.isPriority(), groupId);
		for (AckContext context : nackContexts) {
			boolean offered = getMessageQueue(tpp.getTopic(), tpp.getPartition()).offer(
			      new Operation(key, resend, Type.NACK, context.getMsgSeq(), m_systemClockService.now()));
			logIfOfferFail("nacked", offered);
		}
	}

	private void logIfOfferFail(String type, boolean offered) {
		if (!offered) {
			log.warn("Operation queue full when doing {}", type);
		}
	}

	private void resetPriorityIfResend(Tpp tpp, boolean resend) {
		if (resend) {
			tpp.setPriority(false);
		}
	}

	public static class Operation {
		public enum Type {
			ACK, NACK, DELIVERED;
		}

		// priority, group
		private Pair<Boolean, String> m_key;

		private boolean m_resend;

		private Object m_data;

		private Type m_type;

		private long m_createTime;

		Operation(Pair<Boolean, String> key, boolean isResend, Type type, Object data, long createTime) {
			m_key = key;
			m_resend = isResend;
			m_data = data;
			m_type = type;
			m_createTime = createTime;
		}

		public boolean isResend() {
			return m_resend;
		}

		public Pair<Boolean, String> getKey() {
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

	@Override
	public void initialize() throws InitializationException {
		m_ackOpExecutor = Executors.newScheduledThreadPool(m_config.getAckOpExecutorThreadCount(), HermesThreadFactory.create("AckOp", true));
	}

}
