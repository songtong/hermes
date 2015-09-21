package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.Collections;
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

import com.alibaba.fastjson.JSON;
import com.codahale.metrics.Timer.Context;
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
import com.ctrip.hermes.producer.status.ProducerStatusMonitor;
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
		m_taskQueues.putIfAbsent(tp,
		      new TaskQueue(msg.getTopic(), msg.getPartition(), m_config.getBrokerSenderTaskQueueSize()));

		return m_taskQueues.get(tp).submit(msg);
	}

	private void startEndpointSender() {
		Executors.newSingleThreadExecutor(HermesThreadFactory.create("ProducerEndpointSender", false)).submit(
		      new EndpointSender());
	}

	private class EndpointSender implements Runnable {

		private ConcurrentMap<Pair<String, Integer>, AtomicBoolean> m_runnings = new ConcurrentHashMap<Pair<String, Integer>, AtomicBoolean>();

		private ExecutorService m_taskExecThreadPool;

		public EndpointSender() {
			int threadCount = m_config.getBrokerSenderNetworkIoThreadCount();

			m_taskExecThreadPool = Executors.newFixedThreadPool(threadCount,
			      HermesThreadFactory.create("BrokerMessageSender", false));
		}

		@Override
		public void run() {
			int checkIntervalBase = m_config.getBrokerSenderNetworkIoCheckIntervalBaseMillis();
			int checkIntervalMax = m_config.getBrokerSenderNetworkIoCheckIntervalMaxMillis();

			SchedulePolicy schedulePolicy = new ExponentialSchedulePolicy(checkIntervalBase, checkIntervalMax);

			while (!Thread.currentThread().isInterrupted()) {
				boolean hasTask = scanAndExecuteTasks();
				if (hasTask) {
					schedulePolicy.succeess();
				} else {
					schedulePolicy.fail(true);
				}
			}

		}

		private boolean scanAndExecuteTasks() {
			boolean hasTask = false;

			List<Map.Entry<Pair<String, Integer>, TaskQueue>> entries = new ArrayList<>(m_taskQueues.entrySet());

			Collections.shuffle(entries);
			for (Map.Entry<Pair<String, Integer>, TaskQueue> entry : entries) {
				try {
					TaskQueue queue = entry.getValue();

					if (queue.hasTask()) {
						hasTask = hasTask || scheduleTaskExecution(entry.getKey(), queue);
					}
				} catch (RuntimeException e) {
					// ignore
					log.warn("Exception occurred, ignore it", e);
				}
			}

			return hasTask;
		}

		private boolean scheduleTaskExecution(Pair<String, Integer> tp, TaskQueue queue) {
			m_runnings.putIfAbsent(tp, new AtomicBoolean(false));

			if (m_runnings.get(tp).compareAndSet(false, true)) {
				m_taskExecThreadPool.submit(new SendTask(tp.getKey(), tp.getValue(), queue, m_runnings.get(tp)));
				return true;
			}
			return false;
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
				int batchSize = m_config.getBrokerSenderBatchSize();
				SendMessageCommand cmd = m_taskQueue.pop(batchSize);

				if (cmd != null) {
					Context wholeSendProcessTimer = ProducerStatusMonitor.INSTANCE.getTimer(cmd.getTopic(),
					      cmd.getPartition(), "send-message-duration").time();

					if (!sendMessagesToBroker(cmd)) {
						m_taskQueue.push(cmd);
					}

					wholeSendProcessTimer.stop();
				}
			} finally {
				m_running.set(false);
			}

		}

		private boolean sendMessagesToBroker(SendMessageCommand cmd) {
			try {
				Endpoint endpoint = m_endpointManager.getEndpoint(m_topic, m_partition);
				if (endpoint != null) {
					long correlationId = cmd.getHeader().getCorrelationId();

					Future<Boolean> acceptFuture = m_messageAcceptanceMonitor.monitor(correlationId);
					Future<Boolean> resultFuture = m_messageResultMonitor.monitor(cmd);

					long timeout = m_config.getBrokerSenderSendTimeoutMillis();

					Context acceptTimer = ProducerStatusMonitor.INSTANCE.getTimer(cmd.getTopic(), cmd.getPartition(),
					      "broker-accept-duration").time();

					m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);
					ProducerStatusMonitor.INSTANCE.wroteToBroker(m_topic, m_partition, cmd.getMessageCount());

					Boolean brokerAccepted = null;
					try {
						brokerAccepted = acceptFuture.get(timeout, TimeUnit.MILLISECONDS);
					} catch (TimeoutException e) {
						ProducerStatusMonitor.INSTANCE.waitBrokerAcceptanceTimeout(m_topic, m_partition,
						      cmd.getMessageCount());
						m_messageAcceptanceMonitor.cancel(correlationId);
					}

					acceptTimer.stop();

					if (brokerAccepted != null && brokerAccepted) {
						ProducerStatusMonitor.INSTANCE.brokerAccepted(m_topic, m_partition, cmd.getMessageCount());

						return waitForBrokerResult(cmd, resultFuture);
					} else {
						ProducerStatusMonitor.INSTANCE.brokerRejected(m_topic, m_partition, cmd.getMessageCount());
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
				ProducerStatusMonitor.INSTANCE.sendFailed(m_topic, m_partition, cmd.getMessageCount());
				log.warn("Exception occurred while sending message to broker, will retry it", e);
				return false;
			}
		}

		private boolean waitForBrokerResult(SendMessageCommand cmd, Future<Boolean> resultFuture)
		      throws InterruptedException, ExecutionException {
			try {
				Boolean result = resultFuture.get(m_config.getSendMessageReadResultTimeoutMillis(), TimeUnit.MILLISECONDS);
				return result != null && result.booleanValue();
			} catch (TimeoutException e) {
				m_messageResultMonitor.cancel(cmd);
			}

			return false;
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

			ProducerStatusMonitor.INSTANCE.addTaskQueueGauge(m_topic, m_partition, m_queue);
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
			ProducerStatusMonitor.INSTANCE.messageSubmitted(m_topic, m_partition);
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
				ProducerStatusMonitor.INSTANCE.offerFailed(m_topic, m_partition);
				String body = null;
				try {
					body = JSON.toJSONString(msg);
				} catch (Exception e) {
					body = msg.toString();
				}
				String warning = String.format(
				      "Producer task queue is full(queueSize=%s), will drop this message(refKey=%s, body=%s).",
				      m_queue.size(), msg.getKey(), body);
				log.warn(warning);
				MessageSendException throwable = new MessageSendException(warning);
				future.setException(throwable);
			}
		}
	}

	@Override
	public void initialize() throws InitializationException {
		int callbackThreadCount = m_config.getProducerCallbackThreadCount();

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
