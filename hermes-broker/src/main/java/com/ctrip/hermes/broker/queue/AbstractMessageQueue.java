package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueue implements MessageQueue {

	protected String m_topic;

	protected int m_partition;

	protected AtomicReference<MessageQueueDumper> m_dumper = new AtomicReference<>(null);

	protected MessageQueueStorage m_storage;

	protected SystemClockService m_systemClockService;

	protected ConcurrentMap<String, AtomicReference<MessageQueueCursor>> m_cursors = new ConcurrentHashMap<>();

	public AbstractMessageQueue(String topic, int partition, MessageQueueStorage storage,
	      SystemClockService systemClockService) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
		m_systemClockService = systemClockService;
	}

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageBatchWithRawData batch,
	      Lease lease) {
		MessageQueueDumper existingDumper = m_dumper.get();
		if (existingDumper == null || existingDumper.getLease().getId() != lease.getId()) {
			MessageQueueDumper newDumper = createDumper(lease);
			if (m_dumper.compareAndSet(existingDumper, newDumper)) {
				newDumper.start();
			}
		}

		SettableFuture<Map<Integer, Boolean>> future = SettableFuture.create();

		m_dumper.get().submit(future, batch, isPriority);
		return future;
	}

	@Override
	public MessageQueueCursor getCursor(String groupId, Lease lease) {
		m_cursors.putIfAbsent(groupId, new AtomicReference<MessageQueueCursor>(null));

		MessageQueueCursor existingCursor = m_cursors.get(groupId).get();

		if (existingCursor == null || existingCursor.getLease().getId() != lease.getId()) {
			MessageQueueCursor newCursor = create(groupId, lease);
			if (m_cursors.get(groupId).compareAndSet(existingCursor, newCursor)) {
				newCursor.init();
			}
		}

		return m_cursors.get(groupId).get();
	}

	@Override
	public void nack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, Integer>> msgSeqs) {
		doNack(resend, isPriority, groupId, msgSeqs);
	}

	@Override
	public void ack(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		doAck(resend, isPriority, groupId, msgSeq);
	}

	protected abstract MessageQueueDumper createDumper(Lease lease);

	protected abstract MessageQueueCursor create(String groupId, Lease lease);

	protected abstract void doNack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, Integer>> msgSeqs);

	protected abstract void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq);
}
