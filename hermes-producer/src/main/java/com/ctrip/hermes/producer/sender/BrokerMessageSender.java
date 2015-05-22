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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageSender.class, value = Endpoint.BROKER)
public class BrokerMessageSender extends AbstractMessageSender implements MessageSender {

	private static Logger log = LoggerFactory.getLogger(BrokerMessageSender.class);

	@Inject
	private ProducerConfig m_config;

	private ConcurrentMap<Pair<String, Integer>, TaskQueue> m_taskQueues = new ConcurrentHashMap<>();

	private AtomicBoolean m_workerStarted = new AtomicBoolean(false);

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {
		if (m_workerStarted.compareAndSet(false, true)) {
			startEndpointSender();
		}

		Pair<String, Integer> tp = new Pair<String, Integer>(msg.getTopic(), msg.getPartition());
		m_taskQueues.putIfAbsent(tp,
		      new TaskQueue(msg.getTopic(), msg.getPartition(), m_config.getBrokerSenderTopicPartitionTaskQueueSize()));

		return m_taskQueues.get(tp).submit(msg);
	}

	private void startEndpointSender() {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("ProducerEndpointSender", false))
		      .scheduleWithFixedDelay(new EndpointSender(), 0, m_config.getBrokerSenderCheckInterval(),
		            TimeUnit.MILLISECONDS);
	}

	private class EndpointSender implements Runnable {

		private ConcurrentMap<Pair<String, Integer>, AtomicBoolean> m_runnings = new ConcurrentHashMap<>();

		private ExecutorService m_taskExecThreadPool;

		public EndpointSender() {
			m_taskExecThreadPool = Executors.newFixedThreadPool(m_config.getBrokerSenderTaskExecThreadCount(),
			      HermesThreadFactory.create("BrokerMessageSender", false));
		}

		@Override
		public void run() {
			try {
				for (Map.Entry<Pair<String, Integer>, TaskQueue> entry : m_taskQueues.entrySet()) {
					try {
						TaskQueue queue = entry.getValue();

						if (queue.hasTask()) {
							scheduleTaskExecution(entry.getKey(), queue);
						}
					} catch (Exception e) {
						// ignore
						if (log.isDebugEnabled()) {
							log.debug("Exception occured, ignore it", e);
						}
					}
				}
			} catch (Exception e) {
				// ignore
				if (log.isDebugEnabled()) {
					log.debug("Exception occured, ignore it", e);
				}
			}

		}

		private void scheduleTaskExecution(Pair<String, Integer> tp, TaskQueue queue) {
			m_runnings.putIfAbsent(tp, new AtomicBoolean(false));

			if (m_runnings.get(tp).compareAndSet(false, true)) {
				m_taskExecThreadPool.submit(new SendTask(tp.getKey(), tp.getValue(), queue, m_runnings.get(tp)));
			}
		}

	}

	private class SendTask implements Runnable {

		private String m_topic;

		private int m_partition;

		private TaskQueue m_taskQueue;

		private AtomicBoolean m_running;

		public SendTask(String topic, int partition, TaskQueue taskQueue, AtomicBoolean running) {
			m_topic = topic;
			m_partition = partition;
			m_taskQueue = taskQueue;
			m_running = running;
		}

		@Override
		public void run() {
			try {
				SendMessageCommand cmd = m_taskQueue.peek(m_config.getBrokerSenderBatchSize());

				if (cmd != null && sendMessagesToBroker(cmd)) {
					m_taskQueue.pop();
				}
			} catch (Exception e) {
				// ignore
				if (log.isDebugEnabled()) {
					log.debug("Exception occured, ignore it", e);
				}
			} finally {
				m_running.set(false);
			}

		}

		private boolean sendMessagesToBroker(SendMessageCommand cmd) throws InterruptedException, ExecutionException,
		      TimeoutException {
			Endpoint endpoint = m_endpointManager.getEndpoint(m_topic, m_partition);
			if (endpoint != null) {
				Future<Boolean> future = m_messageAcceptanceMonitor.monitor(cmd.getHeader().getCorrelationId());
				m_messageResultMonitor.monitor(cmd);

				m_endpointClient.writeCommand(endpoint, cmd, m_config.getBrokerSenderSendTimeout(), TimeUnit.MILLISECONDS);

				Boolean brokerAccepted = null;
				try {
					brokerAccepted = future.get(m_config.getBrokerSenderSendTimeout(), TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					future.cancel(true);
				}

				if (brokerAccepted != null && brokerAccepted) {
					return true;
				} else {
					return false;
				}
			} else {
				// ignore
				if (log.isDebugEnabled()) {
					log.debug("No endpoint found, ignore it");
				}
				return false;
			}
		}
	}

	private static class TaskQueue {
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
