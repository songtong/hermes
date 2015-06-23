package com.ctrip.hermes.broker.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.log.BizLogger;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueueDumper implements MessageQueueDumper {

	private static final Logger log = LoggerFactory.getLogger(AbstractMessageQueueDumper.class);

	protected BizLogger m_bizLogger;

	private BlockingQueue<FutureBatchPriorityWrapper> m_queue = new LinkedBlockingQueue<>();

	private AtomicBoolean m_started = new AtomicBoolean(false);

	protected BrokerConfig m_config;

	protected String m_topic;

	protected int m_partition;

	protected Lease m_lease;

	private Thread m_workerThread;

	public AbstractMessageQueueDumper(String topic, int partition, BrokerConfig config, Lease lease) {
		m_topic = topic;
		m_partition = partition;
		m_lease = lease;
		m_config = config;
		m_bizLogger = PlexusComponentLocator.lookup(BizLogger.class);

		String threadName = String.format("MessageQueueDumper-%s-%d", topic, partition);
		m_workerThread = HermesThreadFactory.create(threadName, false).newThread(new DumperTask());
	}

	public Lease getLease() {
		return m_lease;
	}

	private boolean flushMsgs(List<FutureBatchPriorityWrapper> todos) {
		if (todos.isEmpty()) {
			m_queue.drainTo(todos, m_config.getDumperBatchSize());
		}

		if (!todos.isEmpty()) {
			appendMessageSync(todos);
			todos.clear();
			return true;
		} else {
			return false;
		}
	}

	public void submit(SettableFuture<Map<Integer, Boolean>> future, MessageBatchWithRawData batch, boolean isPriority) {
		m_queue.offer(new FutureBatchPriorityWrapper(future, batch, isPriority));
	}

	public void start() {
		if (m_started.compareAndSet(false, true)) {
			m_workerThread.start();
		}
	}

	protected void appendMessageSync(List<FutureBatchPriorityWrapper> todos) {

		List<FutureBatchResultWrapper> priorityTodos = new ArrayList<>();
		List<FutureBatchResultWrapper> nonPriorityTodos = new ArrayList<>();

		for (FutureBatchPriorityWrapper todo : todos) {
			Map<Integer, Boolean> result = new HashMap<>();
			addResults(result, todo.getBatch().getMsgSeqs(), false);

			if (todo.isPriority()) {
				priorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			} else {
				nonPriorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			}
		}

		Function<FutureBatchResultWrapper, Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> transformFucntion = new Function<FutureBatchResultWrapper, Pair<MessageBatchWithRawData, Map<Integer, Boolean>>>() {

			@Override
			public Pair<MessageBatchWithRawData, Map<Integer, Boolean>> apply(FutureBatchResultWrapper input) {
				return new Pair<MessageBatchWithRawData, Map<Integer, Boolean>>(input.getBatch(), input.getResult());
			}
		};

		doAppendMessageSync(true, Collections2.transform(priorityTodos, transformFucntion));

		doAppendMessageSync(false, Collections2.transform(nonPriorityTodos, transformFucntion));

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

	protected abstract void doAppendMessageSync(boolean isPriority,
	      Collection<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> todos);

	private class DumperTask implements Runnable {

		@Override
		public void run() {
			List<FutureBatchPriorityWrapper> todos = new ArrayList<>(m_config.getDumperBatchSize());

			while (!Thread.currentThread().isInterrupted() && !m_lease.isExpired()) {
				try {
					if (!flushMsgs(todos)) {
						TimeUnit.MILLISECONDS.sleep(m_config.getDumperNoMessageWaitIntervalMillis());
					}

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					log.error("Exception occurred while dumping data", e);
				}
			}

			// lease is expired, flush remaining msgs
			while (!m_queue.isEmpty() || !todos.isEmpty()) {
				flushMsgs(todos);
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

	private static class FutureBatchPriorityWrapper {
		private SettableFuture<Map<Integer, Boolean>> m_future;

		private MessageBatchWithRawData m_batch;

		private boolean m_isPriority;

		public FutureBatchPriorityWrapper(SettableFuture<Map<Integer, Boolean>> future, MessageBatchWithRawData batch,
		      boolean isPriority) {
			m_future = future;
			m_batch = batch;
			m_isPriority = isPriority;
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

	}
}
