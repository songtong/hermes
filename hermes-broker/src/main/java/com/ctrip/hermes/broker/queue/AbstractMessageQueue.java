package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
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

	protected ConcurrentMap<String, AtomicReference<MessageQueueCursor>> m_cursors = new ConcurrentHashMap<>();

	protected AtomicBoolean m_stopped = new AtomicBoolean(false);

	public AbstractMessageQueue(String topic, int partition, MessageQueueStorage storage) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
	}

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageBatchWithRawData batch,
	      Lease lease) {
		if (m_stopped.get()) {
			return null;
		}

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
		if (m_stopped.get()) {
			return null;
		}

		m_cursors.putIfAbsent(groupId, new AtomicReference<MessageQueueCursor>(null));

		MessageQueueCursor existingCursor = m_cursors.get(groupId).get();

		if (existingCursor == null || existingCursor.getLease().getId() != lease.getId() || existingCursor.hasError()) {
			MessageQueueCursor newCursor = create(groupId, lease);
			if (m_cursors.get(groupId).compareAndSet(existingCursor, newCursor)) {
				newCursor.init();
			}
		}

		MessageQueueCursor cursor = m_cursors.get(groupId).get();

		return cursor.isInited() ? cursor : new NoopMessageQueueCursor();
	}

	@Override
	public void nack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas) {
		if (!m_stopped.get()) {
			doNack(resend, isPriority, groupId, msgId2Metas);
		}
	}

	@Override
	public void ack(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		if (!m_stopped.get()) {
			doAck(resend, isPriority, groupId, msgSeq);
		}
	}

	@Override
	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			MessageQueueDumper dumper = m_dumper.get();
			if (dumper != null) {
				dumper.stop();
			}

			for (AtomicReference<MessageQueueCursor> cursorRef : m_cursors.values()) {
				MessageQueueCursor cursor = cursorRef.get();
				if (cursor != null) {
					cursor.stop();
				}
			}

			doStop();
		}
	}

	protected abstract void doStop();

	protected abstract MessageQueueDumper createDumper(Lease lease);

	protected abstract MessageQueueCursor create(String groupId, Lease lease);

	protected abstract void doNack(boolean resend, boolean isPriority, String groupId,
	      List<Pair<Long, MessageMeta>> msgId2Metas);

	protected abstract void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq);
}
