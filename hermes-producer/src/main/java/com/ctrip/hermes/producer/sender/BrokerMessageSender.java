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

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageSender.class, value = Endpoint.BROKER)
public class BrokerMessageSender extends AbstractMessageSender implements MessageSender, Initializable {

	private static Logger log = LoggerFactory.getLogger(BrokerMessageSender.class);

	@Inject
	private ProducerConfig m_config;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Inject
	private SystemClockService m_systemClockService;

	private ConcurrentMap<Pair<String, Integer>, TaskQueue> m_taskQueues = new ConcurrentHashMap<>();

	private AtomicBoolean m_workerStarted = new AtomicBoolean(false);

	private ExecutorService m_callbackExecutorService;

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {
		if (m_workerStarted.compareAndSet(false, true)) {
			startEndpointSender();
		}

		Pair<String, Integer> tp = new Pair<String, Integer>(msg.getTopic(), msg.getPartition());
		m_taskQueues.putIfAbsent(
		      tp,
		      new TaskQueue(msg.getTopic(), msg.getPartition(), Integer.valueOf(m_clientEnv.getGlobalConfig()
		            .getProperty("producer.sender.taskqueue.size", m_config.getDefaultBrokerSenderTaskQueueSize()))));

		return m_taskQueues.get(tp).submit(msg);
	}

	private void startEndpointSender() {
		long checkInterval = Long.valueOf(m_clientEnv.getGlobalConfig().getProperty("producer.networkio.interval",
		      m_config.getDefaultBrokerSenderNetworkIoCheckIntervalMillis()));

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("ProducerEndpointSender", false))
		      .scheduleWithFixedDelay(new EndpointSender(), 0, checkInterval, TimeUnit.MILLISECONDS);
	}

	private class EndpointSender implements Runnable {

		private ConcurrentMap<Pair<String, Integer>, AtomicBoolean> m_runnings = new ConcurrentHashMap<>();

		private ExecutorService m_taskExecThreadPool;

		public EndpointSender() {
			int threadCount = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty("producer.networkio.threadcount",
			      m_config.getDefaultBrokerSenderNetworkIoThreadCount()));

			m_taskExecThreadPool = Executors.newFixedThreadPool(threadCount,
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
				int batchSize = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty("producer.sender.batchsize",
				      m_config.getDefaultBrokerSenderBatchSize()));
				SendMessageCommand cmd = m_taskQueue.peek(batchSize);

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
				cmd.setExpireTime(m_systemClockService.now() + m_config.getSendMessageReadResultTimeoutMillis());
				Future<Boolean> future = m_messageAcceptanceMonitor.monitor(cmd.getHeader().getCorrelationId());
				m_messageResultMonitor.monitor(cmd);

				long timeout = Long.valueOf(m_clientEnv.getGlobalConfig().getProperty("producer.sender.send.timeout",
				      m_config.getDefaultBrokerSenderSendTimeoutMillis()));

				m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);

				Boolean brokerAccepted = null;
				try {
					brokerAccepted = future.get(timeout, TimeUnit.MILLISECONDS);
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
			List<ProducerWorkerContext> contexts = new ArrayList<>(size);
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

		public Future<SendResult> submit(final ProducerMessage<?> msg) {
			SettableFuture<SendResult> future = SettableFuture.create();

			m_queue.offer(new ProducerWorkerContext(msg, future));

			if (msg.getCallback() != null) {
				Futures.addCallback(future, new FutureCallback<SendResult>() {

					@Override
					public void onSuccess(SendResult result) {
						msg.getCallback().onSuccess(result);
					}

					@Override
					public void onFailure(Throwable t) {
						msg.getCallback().onFailure(t);
					}
				}, m_callbackExecutorService);
			}

			return future;
		}

	}

	@Override
	public void initialize() throws InitializationException {
		int callbackThreadCount = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty(
		      "producer.callback.threadcount", m_config.getDefaultProducerCallbackThreadCount()));

		m_callbackExecutorService = Executors.newFixedThreadPool(callbackThreadCount,
		      HermesThreadFactory.create("ProducerCallback", false));
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
