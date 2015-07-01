package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

@Named(type = MessageQueueManager.class)
public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager {

	@Inject
	private MessageQueuePartitionFactory m_queueFactory;

	// one <topic, partition, lease> mapping to one MessageQueue
	private Map<Pair<String, Integer>, MessageQueue> m_messageQueues = new ConcurrentHashMap<>();

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

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
					MessageQueue mqp = m_queueFactory.getMessageQueue(topic, partition);
					m_messageQueues.put(key, mqp);
				}
			}
		}

		return m_messageQueues.get(key);
	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		if (!m_stopped.get()) {
			getMessageQueue(tpp.getTopic(), tpp.getPartition()).ack(resend, tpp.isPriority(), groupId, msgSeq);
		}
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas) {
		if (!m_stopped.get()) {
			getMessageQueue(tpp.getTopic(), tpp.getPartition()).nack(resend, tpp.isPriority(), groupId, msgId2Metas);
		}
	}

	@Override
	public void stop() {
		for (MessageQueue mq : m_messageQueues.values()) {
			mq.stop();
		}
	}
}
