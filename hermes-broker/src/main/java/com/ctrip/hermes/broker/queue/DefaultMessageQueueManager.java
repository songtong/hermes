package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

@Named(type = MessageQueueManager.class)
public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager {

	@Inject
	private MessageQueuePartitionFactory m_queueFactory;

	// one <topic, partition, lease> mapping to one MessageQueue
	private Map<Pair<String, Integer>, MessageQueue> m_messageQueues = new ConcurrentHashMap<>();

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageBatchWithRawData data, Lease lease) {
		return getMessageQueue(tpp.getTopic(), tpp.getPartition()).appendMessageAsync(tpp.isPriority(), data, lease);
	}

	@Override
	public MessageQueueCursor getCursor(Tpg tpg, Lease lease) {
		return getMessageQueue(tpg.getTopic(), tpg.getPartition()).getCursor(tpg.getGroupId(), lease);
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
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqs) {
		getMessageQueue(tpp.getTopic(), tpp.getPartition()).nack(resend, tpp.isPriority(), groupId, msgSeqs);
	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		getMessageQueue(tpp.getTopic(), tpp.getPartition()).ack(resend, tpp.isPriority(), groupId, msgSeq);
	}
}
