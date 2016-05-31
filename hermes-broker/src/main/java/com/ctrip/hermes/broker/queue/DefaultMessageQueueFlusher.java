package com.ctrip.hermes.broker.queue;

import java.lang.reflect.InvocationTargetException;
import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.mysql.jdbc.PacketTooBigException;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueueFlusher implements MessageQueueFlusher {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueFlusher.class);

	private static final Logger skipLog = LoggerFactory.getLogger("MessageSkip");

	private static FutureBatchResultWrapperTransformer m_futureBatchResultWrapperTransformer = new FutureBatchResultWrapperTransformer();

	private static MessageBatchAndResultTransformer m_messageBatchAndResultTransformer = new MessageBatchAndResultTransformer();

	protected FileBizLogger m_bizLogger;

	private String m_topic;

	private int m_partition;

	private MessageQueueStorage m_storage;

	private BlockingQueue<PendingMessageWrapper> m_pendingMessages = new LinkedBlockingQueue<>();

	private MetaService m_metaService;

	private AtomicBoolean m_flushing = new AtomicBoolean(false);

	public DefaultMessageQueueFlusher(String topic, int partition, MessageQueueStorage storage, MetaService metaService) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
		m_bizLogger = PlexusComponentLocator.lookup(FileBizLogger.class);
		m_metaService = metaService;
	}

	@Override
	public boolean hasUnflushedMessages() {
		return !m_pendingMessages.isEmpty();
	}

	@Override
	public boolean startFlush() {
		return m_flushing.compareAndSet(false, true);
	}

	@Override
	public void flush(int batchSize) {
		purgeExpiredMsgs();

		if (!m_pendingMessages.isEmpty()) {
			List<PendingMessageWrapper> todos = new ArrayList<>(batchSize < 0 ? m_pendingMessages.size() : batchSize);

			if (batchSize < 0) {
				m_pendingMessages.drainTo(todos);
			} else {
				m_pendingMessages.drainTo(todos, batchSize);
			}

			Transaction catTx = Cat.newTransaction(CatConstants.TYPE_MESSAGE_BROKER_FLUSH, m_topic + "-" + m_partition);
			catTx.addData("*count", getMessageCount(todos));

			appendMessageSync(todos);
			catTx.setStatus(Transaction.SUCCESS);
			catTx.complete();
		}
	}

	private int getMessageCount(List<PendingMessageWrapper> todos) {
		int count = 0;
		for (PendingMessageWrapper todo : todos) {
			count += todo.getBatch().getMsgSeqs().size();
		}
		return count;
	}

	private void purgeExpiredMsgs() {
		long now = System.currentTimeMillis();

		while (!m_pendingMessages.isEmpty()) {
			if (m_pendingMessages.peek().getExpireTime() < now) {
				purgeExpiredMsg();
			} else {
				break;
			}
		}
	}

	private void purgeExpiredMsg() {
		PendingMessageWrapper messageWrapper = m_pendingMessages.poll();
		if (messageWrapper != null && messageWrapper.getBatch() != null) {
			Map<Integer, Boolean> result = new HashMap<>();
			addResults(result, messageWrapper.getBatch().getMsgSeqs(), false);
			messageWrapper.getFuture().set(result);
		}
	}

	@Override
	public void finishFlush() {
		m_flushing.set(false);
	}

	private static class FutureBatchResultWrapperTransformer implements
	      Function<FutureBatchResultWrapper, Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> {
		@Override
		public Pair<MessageBatchWithRawData, Map<Integer, Boolean>> apply(FutureBatchResultWrapper input) {
			return new Pair<MessageBatchWithRawData, Map<Integer, Boolean>>(input.getBatch(), input.getResult());
		}
	}

	protected void appendMessageSync(List<PendingMessageWrapper> todos) {

		List<FutureBatchResultWrapper> priorityTodos = new ArrayList<>(todos.size());
		List<FutureBatchResultWrapper> nonPriorityTodos = new ArrayList<>(todos.size());

		for (PendingMessageWrapper todo : todos) {
			Map<Integer, Boolean> result = new HashMap<>();
			addResults(result, todo.getBatch().getMsgSeqs(), false);

			if (todo.isPriority()) {
				priorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			} else {
				nonPriorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			}
		}

		doAppendMessageSync(true, Collections2.transform(priorityTodos, m_futureBatchResultWrapperTransformer));

		doAppendMessageSync(false, Collections2.transform(nonPriorityTodos, m_futureBatchResultWrapperTransformer));

		for (List<FutureBatchResultWrapper> todo : Arrays.asList(priorityTodos, nonPriorityTodos)) {
			for (FutureBatchResultWrapper fbw : todo) {
				SettableFuture<Map<Integer, Boolean>> future = fbw.getFuture();
				Map<Integer, Boolean> result = fbw.getResult();
				future.set(result);
			}
		}

	}

	protected void addResults(Map<Integer, Boolean> result, List<Integer> seqs, boolean success) {
		for (Integer seq : seqs) {
			result.put(seq, success);
		}
	}

	protected void addResults(Map<Integer, Boolean> result, boolean success) {
		for (Integer key : result.keySet()) {
			result.put(key, success);
		}
	}

	@Override
	public ListenableFuture<Map<Integer, Boolean>> append(boolean isPriority, MessageBatchWithRawData batch,
	      long expireTime) {
		SettableFuture<Map<Integer, Boolean>> future = SettableFuture.create();

		m_pendingMessages.offer(new PendingMessageWrapper(future, batch, isPriority, expireTime));

		return future;
	}

	private static class MessageBatchAndResultTransformer implements
	      Function<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>, MessageBatchWithRawData> {
		@Override
		public MessageBatchWithRawData apply(Pair<MessageBatchWithRawData, Map<Integer, Boolean>> input) {
			return input.getKey();
		}
	}

	protected void doAppendMessageSync(boolean isPriority,
	      Collection<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> todos) {
		Collection<MessageBatchWithRawData> batches = null;
		try {
			batches = Collections2.transform(todos, m_messageBatchAndResultTransformer);
			m_storage.appendMessages(m_topic, m_partition, isPriority, batches);

			setBatchesResult(isPriority, todos, true);
		} catch (Exception e) {
			if (shoudlSkip(e)) {
				setBatchesResult(isPriority, todos, true);
				for (MessageBatchWithRawData batch : batches) {
					for (PartialDecodedMessage msg : batch.getMessages()) {
						skipLog.info("Message too large: {} {}\n{}", m_topic, m_partition, Arrays.toString(msg.readBody()));
					}
				}
			} else {
				setBatchesResult(isPriority, todos, false);
				log.error("Failed to append messages.", e);
			}
		}
	}

	private boolean shoudlSkip(Exception e) {
		if (e instanceof InvocationTargetException) {
			InvocationTargetException ite = (InvocationTargetException) e;
			if (ite.getTargetException() instanceof DalException) {
				DalException de = (DalException) ite.getTargetException();
				if (de.getCause() instanceof BatchUpdateException) {
					BatchUpdateException bue = (BatchUpdateException) de.getCause();
					if (bue.getCause() instanceof PacketTooBigException) {
						return true;
					}
				}
			}
		}

		return false;
	}

	private void setBatchesResult(boolean isPriority,
	      Collection<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> todos, boolean success) {
		for (Pair<MessageBatchWithRawData, Map<Integer, Boolean>> todo : todos) {
			bizLog(isPriority, todo.getKey(), success);
			Map<Integer, Boolean> result = todo.getValue();
			addResults(result, success);
		}
	}

	private void bizLog(boolean isPriority, MessageBatchWithRawData batch, boolean success) {
		if (!Storage.KAFKA.equals(m_metaService.findTopicByName(batch.getTopic()).getStorageType())) {
			for (PartialDecodedMessage msg : batch.getMessages()) {
				BizEvent event = new BizEvent("Message.Saved");
				event.addData("topic", m_metaService.findTopicByName(batch.getTopic()).getId());
				event.addData("partition", m_partition);
				event.addData("priority", isPriority ? 0 : 1);
				event.addData("refKey", msg.getKey());
				event.addData("success", success);

				m_bizLogger.log(event);
			}
		}
	}

	private static class FutureBatchResultWrapper {
		private SettableFuture<Map<Integer, Boolean>> m_future;

		private MessageBatchWithRawData m_batch;

		private Map<Integer, Boolean> m_result;

		public FutureBatchResultWrapper(SettableFuture<Map<Integer, Boolean>> future, MessageBatchWithRawData batch,
		      Map<Integer, Boolean> result) {
			m_future = future;
			m_batch = batch;
			m_result = result;
		}

		public SettableFuture<Map<Integer, Boolean>> getFuture() {
			return m_future;
		}

		public MessageBatchWithRawData getBatch() {
			return m_batch;
		}

		public Map<Integer, Boolean> getResult() {
			return m_result;
		}

	}

	private static class PendingMessageWrapper {
		private SettableFuture<Map<Integer, Boolean>> m_future;

		private MessageBatchWithRawData m_batch;

		private boolean m_isPriority;

		private long m_expireTime;

		public PendingMessageWrapper(SettableFuture<Map<Integer, Boolean>> future, MessageBatchWithRawData batch,
		      boolean isPriority, long expireTime) {
			m_future = future;
			m_batch = batch;
			m_isPriority = isPriority;
			m_expireTime = expireTime;
		}

		public SettableFuture<Map<Integer, Boolean>> getFuture() {
			return m_future;
		}

		public MessageBatchWithRawData getBatch() {
			return m_batch;
		}

		public boolean isPriority() {
			return m_isPriority;
		}

		public long getExpireTime() {
			return m_expireTime;
		}

	}

}
