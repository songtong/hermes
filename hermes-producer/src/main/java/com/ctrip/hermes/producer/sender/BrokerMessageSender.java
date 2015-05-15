package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageSender.class, value = Endpoint.BROKER)
public class BrokerMessageSender extends AbstractMessageSender implements MessageSender {

	private ConcurrentMap<Pair<String, Integer>, TaskQueue> m_taskQueues = new ConcurrentHashMap<>();

	private AtomicBoolean m_workerStarted = new AtomicBoolean(false);

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {
		if (m_workerStarted.compareAndSet(false, true)) {
			// TODO config
			startEndpointSender(10, 50, 3000, 50L);
		}

		Pair<String, Integer> tp = new Pair<String, Integer>(msg.getTopic(), msg.getPartitionNo());
		// TODO queueSize
		m_taskQueues.putIfAbsent(tp, new TaskQueue(msg.getTopic(), msg.getPartitionNo(), 10000));

		return m_taskQueues.get(tp).submit(msg);
	}

	private void startEndpointSender(int taskExecThreadCount, long interval, int batchSize, long timeout) {
		// TODO
		Thread t = new Thread(new EndpointSender(taskExecThreadCount, batchSize, timeout, interval));
		t.setName("ProducerEndpointSender");
		t.setDaemon(true);
		t.start();
	}

	private class EndpointSender implements Runnable {

		private ConcurrentMap<Pair<String, Integer>, AtomicBoolean> m_runnings = new ConcurrentHashMap<>();

		private int m_batchSize;

		private long m_timeout;

		private long m_interval;

		private ExecutorService m_taskExecThreadPool;

		public EndpointSender(int taskExecThreadCount, int batchSize, long timeout, long interval) {
			m_batchSize = batchSize;
			m_timeout = timeout;
			m_interval = interval;

			m_taskExecThreadPool = Executors.newFixedThreadPool(taskExecThreadCount, new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "SenderTask");
				}
			});
		}

		@Override
		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				for (Map.Entry<Pair<String, Integer>, TaskQueue> entry : m_taskQueues.entrySet()) {
					try {
						TaskQueue queue = entry.getValue();

						if (queue.hasTask()) {
							scheduleTaskExecution(entry.getKey(), queue);
						}
					} catch (Exception e) {
						// TODO
						e.printStackTrace();
					}
				}

				try {
					TimeUnit.MILLISECONDS.sleep(m_interval);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}

		private void scheduleTaskExecution(Pair<String, Integer> key, TaskQueue queue) {
			m_runnings.putIfAbsent(key, new AtomicBoolean(false));

			if (m_runnings.get(key).compareAndSet(false, true)) {
				m_taskExecThreadPool.submit(new SendTask(key.getKey(), key.getValue(), queue, m_batchSize, m_timeout,
				      m_runnings.get(key)));
			}
		}

	}

	private class SendTask implements Runnable {

		private String m_topic;

		private int m_partition;

		private TaskQueue m_taskQueue;

		private int m_batchSize;

		private long m_timeout;

		private AtomicBoolean m_running;

		public SendTask(String topic, int partition, TaskQueue taskQueue, int batchSize, long timeout,
		      AtomicBoolean running) {
			m_topic = topic;
			m_partition = partition;
			m_taskQueue = taskQueue;
			m_batchSize = batchSize;
			m_timeout = timeout;
			m_running = running;
		}

		@Override
		public void run() {

			try {

				SendMessageCommand cmd = m_taskQueue.peek(m_batchSize);

				if (cmd != null && sendMessagesToBroker(cmd)) {
					m_taskQueue.pop();
				}

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				// TODO
				e.printStackTrace();
			} finally {
				m_running.set(false);
			}

		}

		private boolean sendMessagesToBroker(SendMessageCommand cmd) throws InterruptedException, ExecutionException,
		      TimeoutException {
			Endpoint endpoint = m_endpointManager.getEndpoint(m_topic, m_partition);
			EndpointChannel channel = m_clientEndpointChannelManager.getChannel(endpoint);

			Future<Boolean> future = m_messageAcceptedMonitor.monitor(cmd.getHeader().getCorrelationId());
			m_messageResultMonitor.monitor(cmd);

			channel.writeCommand(cmd);

			Boolean brokerAccepted = null;
			try {
				brokerAccepted = future.get(m_timeout, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				future.cancel(true);
			}

			if (brokerAccepted != null && brokerAccepted) {
				return true;
			} else {
				return false;
			}
		}
	}

	private class TaskQueue {
		private String m_topic;

		private int m_partition;

		private AtomicReference<SendMessageCommand> m_cmd = new AtomicReference<>(null);

		private BlockingQueue<ProducerWorkerContext> m_queue;

		public TaskQueue(String topic, int partition, int queueSize) {
			m_topic = topic;
			m_partition = partition;
			m_queue = new LinkedBlockingQueue<>(queueSize);
		}

		public void pop() {
			m_cmd.set(null);
		}

		public SendMessageCommand peek(int size) {
			if (m_cmd.get() == null) {
				m_cmd.set(createSendMessageCommand(size));
			}
			return m_cmd.get();
		}

		private SendMessageCommand createSendMessageCommand(int size) {
			SendMessageCommand cmd = null;
			List<ProducerWorkerContext> contexts = new ArrayList<>();
			m_queue.drainTo(contexts, size);
			if (!contexts.isEmpty()) {
				cmd = new SendMessageCommand(m_topic, m_partition);
				for (ProducerWorkerContext context : contexts) {
					cmd.addMessage(context.m_msg, context.m_future);
				}
			}
			return cmd;
		}

		public boolean hasTask() {
			return m_cmd.get() != null || !m_queue.isEmpty();
		}

		public Future<SendResult> submit(ProducerMessage<?> msg) {
			SettableFuture<SendResult> future = SettableFuture.create();

			m_queue.offer(new ProducerWorkerContext(msg, future));

			return future;
		}

	}

	private static class ProducerWorkerContext {
		private ProducerMessage<?> m_msg;

		private SettableFuture<SendResult> m_future;

		public ProducerWorkerContext(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
			m_msg = msg;
			m_future = future;
		}

	}

}
