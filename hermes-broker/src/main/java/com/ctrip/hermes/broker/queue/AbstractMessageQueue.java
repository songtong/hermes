package com.ctrip.hermes.broker.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.ack.internal.AckHolder;
import com.ctrip.hermes.broker.ack.internal.AckHolder.AckHolderType;
import com.ctrip.hermes.broker.ack.internal.BatchResult;
import com.ctrip.hermes.broker.ack.internal.ContinuousRange;
import com.ctrip.hermes.broker.ack.internal.DefaultAckHolder;
import com.ctrip.hermes.broker.ack.internal.EnumRange;
import com.ctrip.hermes.broker.ack.internal.ForwardOnlyAckHolder;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueue implements MessageQueue {

	private static final Logger log = LoggerFactory.getLogger(AbstractMessageQueue.class);

	protected String m_topic;

	protected int m_partition;

	protected AtomicReference<MessageQueueDumper> m_dumper = new AtomicReference<>(null);

	protected MessageQueueStorage m_storage;

	protected ConcurrentMap<String, AtomicReference<MessageQueueCursor>> m_cursors = new ConcurrentHashMap<>();

	protected AtomicBoolean m_stopped = new AtomicBoolean(false);

	// TODO while consumer disconnect, clear holder and offset
	protected Map<Pair<Boolean, String>, AckHolder<MessageMeta>> m_ackHolders;

	protected Map<Pair<Boolean, String>, AckHolder<MessageMeta>> m_resendAckHolders;

	protected Map<Pair<Boolean, String>, AckHolder<MessageMeta>> m_forwardOnlyAckHolders;

	private BlockingQueue<Operation> m_opQueue;

	private AckTask m_ackTask;

	private BrokerConfig m_config;

	private MetaService m_metaService;

	private ScheduledExecutorService m_ackOpExecutor;

	public AbstractMessageQueue(String topic, int partition, MessageQueueStorage storage,
	      ScheduledExecutorService ackOpExecutor) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
		m_ackHolders = new ConcurrentHashMap<>();
		m_resendAckHolders = new ConcurrentHashMap<>();
		m_forwardOnlyAckHolders = new ConcurrentHashMap<>();
		m_ackOpExecutor = ackOpExecutor;
		m_config = PlexusComponentLocator.lookup(BrokerConfig.class);
		m_metaService = PlexusComponentLocator.lookup(MetaService.class);

		init();
	}

	private void init() {
		m_opQueue = new LinkedBlockingQueue<>(m_config.getAckOpQueueSize());
		m_ackTask = new AckTask();
		m_ackOpExecutor.schedule(m_ackTask, m_config.getAckOpCheckIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageBatchWithRawData batch,
	      Lease lease) {
		if (m_stopped.get()) {
			return null;
		}

		MessageQueueDumper existingDumper = m_dumper.get();
		if (existingDumper == null || existingDumper.getLease().getId() != lease.getId() || existingDumper.getLease().isExpired()) {
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
				clearHolders(groupId);
				newCursor.init();
			}
		}

		MessageQueueCursor cursor = m_cursors.get(groupId).get();

		return cursor.isInited() ? cursor : new NoopMessageQueueCursor();
	}

	private void clearHolders(String groupId) {
		m_ackHolders.remove(new Pair<Boolean, String>(true, groupId));
		m_ackHolders.remove(new Pair<Boolean, String>(false, groupId));
		m_resendAckHolders.remove(new Pair<Boolean, String>(true, groupId));
		m_resendAckHolders.remove(new Pair<Boolean, String>(false, groupId));
		m_forwardOnlyAckHolders.remove(new Pair<Boolean, String>(true, groupId));
		m_forwardOnlyAckHolders.remove(new Pair<Boolean, String>(false, groupId));
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

			m_ackTask.run();

			doStop();
		}
	}

	@Override
	public void checkHolders() {
		for (Entry<Pair<Boolean, String>, AckHolder<MessageMeta>> entry : m_forwardOnlyAckHolders.entrySet()) {
			BatchResult<MessageMeta> result = entry.getValue().scan();
			doCheckHolders(entry.getKey(), result, false);
		}

		for (Entry<Pair<Boolean, String>, AckHolder<MessageMeta>> entry : m_ackHolders.entrySet()) {
			BatchResult<MessageMeta> result = entry.getValue().scan();
			doCheckHolders(entry.getKey(), result, false);
		}

		for (Entry<Pair<Boolean, String>, AckHolder<MessageMeta>> entry : m_resendAckHolders.entrySet()) {
			BatchResult<MessageMeta> result = entry.getValue().scan();
			doCheckHolders(entry.getKey(), result, true);
		}
	}

	protected void doCheckHolders(Pair<Boolean, String> pg, BatchResult<MessageMeta> result, boolean isResend) {
		if (result != null) {
			Tpp tpp = new Tpp(m_topic, m_partition, pg.getKey());
			String groupId = pg.getValue();

			ContinuousRange doneRange = result.getDoneRange();
			EnumRange<MessageMeta> failRange = result.getFailRange();
			if (failRange != null) {
				if (log.isDebugEnabled()) {
					log.debug(
					      "Nack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, msgIdToRemainingRetries={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, failRange.getOffsets());
				}

				try {
					doNack(isResend, pg.getKey(), groupId, failRange.getOffsets());
				} catch (Exception e) {
					log.error(
					      "Failed to nack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, msgIdToRemainingRetries={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, failRange.getOffsets(), e);
				}
			}

			if (doneRange != null) {
				if (log.isDebugEnabled()) {
					log.debug("Ack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, endOffset={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, doneRange.getEnd());
				}
				try {
					doAck(isResend, pg.getKey(), groupId, doneRange.getEnd());
				} catch (Exception e) {
					log.error("Ack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, endOffset={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, doneRange.getEnd(), e);
				}
			}
		}
	}

	private class AckTask implements Runnable {
		private List<Operation> m_todos = new ArrayList<Operation>();

		@Override
		public synchronized void run() {
			try {
				handleOperations();
				checkHolders();
			} catch (Exception e) {
				log.error("Exception occurred while executing ack task.", e);
			} finally {
				if (!m_stopped.get()) {
					m_ackOpExecutor
					      .schedule(m_ackTask, m_config.getAckOpCheckIntervalMillis(), TimeUnit.MILLISECONDS);
				}
			}
		}

		@SuppressWarnings("unchecked")
		private void handleOperations() {
			try {
				if (m_todos.isEmpty()) {
					m_opQueue.drainTo(m_todos, m_config.getAckOpHandlingBatchSize());
				}

				if (m_todos.isEmpty()) {
					return;
				}

				for (Operation op : m_todos) {
					AckHolder<MessageMeta> holder = findHolder(op);

					switch (op.getType()) {
					case ACK:
						holder.acked((Long) op.getData(), true);
						break;
					case NACK:
						holder.acked((Long) op.getData(), false);
						break;
					case DELIVERED:
						holder.delivered((List<Pair<Long, MessageMeta>>) op.getData(), op.getCreateTime());
						break;

					default:
						break;
					}
				}

				m_todos.clear();
			} catch (Exception e) {
				log.error("Exception occurred while handling operations.", e);
			}
		}

		private AckHolder<MessageMeta> findHolder(Operation op) {
			Map<Pair<Boolean, String>, AckHolder<MessageMeta>> holders = findHolders(op);

			AckHolder<MessageMeta> holder = null;
			holder = holders.get(op.getKey());
			if (holder == null) {
				int timeout = m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup(m_topic, op.getKey().getValue()) * 1000;

				holder = isForwordOnly(op) ? new ForwardOnlyAckHolder() : new DefaultAckHolder<MessageMeta>(
				      timeout);
				holders.put(op.getKey(), holder);
			}

			return holder;
		}

		private Map<Pair<Boolean, String>, AckHolder<MessageMeta>> findHolders(Operation op) {
			return isForwordOnly(op) ? m_forwardOnlyAckHolders : (op.isResend() ? m_resendAckHolders : m_ackHolders);
		}

		private boolean isForwordOnly(Operation op) {
			return AckHolderType.FORWARD_ONLY == op.getAckHolderType();
		}
	}

	@Override
	public boolean offer(Operation operation) {
		return m_opQueue.offer(operation);
	}

	protected abstract void doStop();

	protected abstract MessageQueueDumper createDumper(Lease lease);

	protected abstract MessageQueueCursor create(String groupId, Lease lease);

	protected abstract void doNack(boolean resend, boolean isPriority, String groupId,
	      List<Pair<Long, MessageMeta>> msgId2Metas);

	protected abstract void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq);
}
