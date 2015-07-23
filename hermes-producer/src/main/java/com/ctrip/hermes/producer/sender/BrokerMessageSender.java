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
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
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

	private ConcurrentMap<Pair<String, Integer>, TaskQueue> m_taskQueues = new ConcurrentHashMap<Pair<String, Integer>, TaskQueue>();

	private ExecutorService m_callbackExecutor;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {
		if (m_started.compareAndSet(false, true)) {
			startEndpointSender();
		}

		Pair<String, Integer> tp = new Pair<String, Integer>(msg.getTopic(), msg.getPartition());
		m_taskQueues.putIfAbsent(
		      tp,
		      new TaskQueue(msg.getTopic(), msg.getPartition(), Integer.valueOf(m_clientEnv.getGlobalConfig()
		            .getProperty("producer.sender.taskqueue.size", m_config.getDefaultBrokerSenderTaskQueueSize()))));

		return m_taskQueues.get(tp).submit(msg);
	}

	@Override
	public void resend(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
		Pair<String, Integer> tp = new Pair<String, Integer>(msg.getTopic(), msg.getPartition());
		m_taskQueues.get(tp).resubmit(msg, future);
	}

	private void startEndpointSender() {
		Executors.newSingleThreadExecutor(HermesThreadFactory.create("ProducerEndpointSender", false)).submit(
		      new EndpointSender());
	}

	private class EndpointSender implements Runnable {

		private ConcurrentMap<Pair<String, Integer>, AtomicBoolean> m_runnings = new ConcurrentHashMap<Pair<String, Integer>, AtomicBoolean>();

		private ExecutorService m_taskExecThreadPool;

		public EndpointSender() {
			int threadCount = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty("producer.networkio.threadcount",
			      m_config.getDefaultBrokerSenderNetworkIoThreadCount()));

			m_taskExecThreadPool = Executors.newFixedThreadPool(threadCount,
			      HermesThreadFactory.create("BrokerMessageSender", false));
		}

		@Override
		public void run() {
			int checkIntervalBase = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty(
			      "producer.networkio.interval.base", m_config.getDefaultBrokerSenderNetworkIoCheckIntervalBaseMillis()));
			int checkIntervalMax = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty(
			      "producer.networkio.interval.max", m_config.getDefaultBrokerSenderNetworkIoCheckIntervalMaxMillis()));

			SchedulePolicy schedulePolicy = new ExponentialSchedulePolicy(checkIntervalBase, checkIntervalMax);
			while (!Thread.currentThread().isInterrupted()) {
				boolean hasTask = scanTaskQueues();
				if (hasTask) {
					schedulePolicy.succeess();
				} else {
					schedulePolicy.fail(true);
				}
			}

		}

		private boolean scanTaskQueues() {
			boolean hasTask = false;
			for (Map.Entry<Pair<String, Integer>, TaskQueue> entry : m_taskQueues.entrySet()) {
				try {
					TaskQueue queue = entry.getValue();

					if (queue.hasTask()) {
						hasTask = true;
						scheduleTaskExecution(entry.getKey(), queue);
					}
				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Exception occurred, ignore it", e);
					}
				}
			}

			return hasTask;
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
				SendMessageCommand cmd = m_taskQueue.pop(batchSize);

				if (cmd != null) {
					if (sendMessagesToBroker(cmd)) {
						cmd.accepted(m_systemClockService.now());
					} else {
						m_taskQueue.push(cmd);
					}
				}
			} catch (Exception e) {
				// ignore
				if (log.isDebugEnabled()) {
					log.debug("Exception occurred, ignore it", e);
				}
			} finally {
				m_running.set(false);
			}

		}

		private boolean sendMessagesToBroker(SendMessageCommand cmd) throws InterruptedException, ExecutionException,
		      TimeoutException {
			try {
				Endpoint endpoint = m_endpointManager.getEndpoint(m_topic, m_partition);
				if (endpoint != null) {
					Future<Boolean> future = m_messageAcceptanceMonitor.monitor(cmd.getHeader().getCorrelationId());
					m_messageResultMonitor.monitor(cmd);

					long timeout = m_config.getDefaultBrokerSenderSendTimeoutMillis();

					m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);

					Boolean brokerAccepted = null;
					try {
						brokerAccepted = future.get(timeout, TimeUnit.MILLISECONDS);
					} catch (Exception e) {
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
			} catch (Exception e) {
				log.warn("Exception occurred while sending message to broker, will retry it", e);
				return false;
			}
		}
	}

	private class TaskQueue {
		private String m_topic;

		private int m_partition;

		private AtomicReference<SendMessageCommand> m_cmd = new AtomicReference<SendMessageCommand>(null);

		private BlockingQueue<ProducerWorkerContext> m_queue;

		public TaskQueue(String topic, int partition, int queueSize) {
			m_topic = topic;
			m_partition = partition;
			m_queue = new LinkedBlockingQueue<ProducerWorkerContext>(queueSize);
		}

		public void push(SendMessageCommand cmd) {
			m_cmd.set(cmd);
		}

		public SendMessageCommand pop(int size) {
			if (m_cmd.get() == null) {
				return createSendMessageCommand(size);
			}
			return m_cmd.getAndSet(null);
		}

		private SendMessageCommand createSendMessageCommand(int size) {
			SendMessageCommand cmd = null;
			List<ProducerWorkerContext> contexts = new ArrayList<ProducerWorkerContext>(size);
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
				}, m_callbackExecutor);
			}

			offer(msg, future);

			return future;
		}

		private void offer(final ProducerMessage<?> msg, SettableFuture<SendResult> future) {
			if (!m_queue.offer(new ProducerWorkerContext(msg, future))) {
				String warning = "Producer task queue is full, will drop this message.";
				log.warn(warning);
				MessageSendException throwable = new MessageSendException(warning);
				future.setException(throwable);
			}
		}

		public void resubmit(final ProducerMessage<?> msg, SettableFuture<SendResult> future) {
			offer(msg, future);
		}

	}

	@Override
	public void initialize() throws InitializationException {
		int callbackThreadCount = Integer.valueOf(m_clientEnv.getGlobalConfig().getProperty(
		      "producer.callback.threadcount", m_config.getDefaultProducerCallbackThreadCount()));

		m_callbackExecutor = Executors.newFixedThreadPool(callbackThreadCount,
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
