package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.command.v5.SendMessageCommandV5;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.status.ProducerStatusMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
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

	private ConcurrentMap<Pair<String, Integer>, TaskQueue> m_taskQueues = new ConcurrentHashMap<Pair<String, Integer>, TaskQueue>();

	private ExecutorService m_callbackExecutor;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {
		if (m_started.compareAndSet(false, true)) {
			int callbackThreadCount = m_config.getProducerCallbackThreadCount();

			m_callbackExecutor = Executors.newFixedThreadPool(callbackThreadCount,
			      HermesThreadFactory.create("ProducerCallback", false));

			startEndpointSender();
		}

		Pair<String, Integer> taskQueueKey = computeTaskQueueKey(msg);

		TaskQueue taskQueue = m_taskQueues.get(taskQueueKey);
		if (taskQueue == null) {
			m_taskQueues.putIfAbsent(taskQueueKey,
			      new TaskQueue(msg.getTopic(), msg.getPartition(), m_config.getBrokerSenderTaskQueueSize()));
			taskQueue = m_taskQueues.get(taskQueueKey);
		}

		return taskQueue.submit(msg);
	}

	private Pair<String, Integer> computeTaskQueueKey(ProducerMessage<?> msg) {
		int pkHashCode = getPartitionKeyHashCode(msg);

		int taskQueueIndex = msg.getPartition()
		      * m_config.getBrokerSenderConcurrentLevel()
		      + ((pkHashCode == Integer.MIN_VALUE ? 0 : Math.abs(pkHashCode)) % m_config.getBrokerSenderConcurrentLevel());

		Pair<String, Integer> taskQueueKey = new Pair<String, Integer>(msg.getTopic(), taskQueueIndex);
		return taskQueueKey;
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
						hasTask = scheduleTaskExecution(entry.getKey(), queue) || hasTask;
					}
				} catch (RuntimeException e) {
					// ignore
					log.warn("Exception occurred, ignore it", e);
				}
			}

			return hasTask;
		}

		private boolean scheduleTaskExecution(Pair<String, Integer> taskQueueKey, TaskQueue queue) {

			AtomicBoolean queueRunning = m_runnings.get(taskQueueKey);
			if (queueRunning == null) {
				m_runnings.putIfAbsent(taskQueueKey, new AtomicBoolean(false));
				queueRunning = m_runnings.get(taskQueueKey);
			}

			if (queueRunning.compareAndSet(false, true)) {
				m_taskExecThreadPool.submit(new SendTask(queue, queueRunning));
				return true;
			}
			return false;
		}
	}

	private class SendTask implements Runnable {

		private TaskQueue m_taskQueue;

		private AtomicBoolean m_running;

		public SendTask(TaskQueue taskQueue, AtomicBoolean running) {
			m_taskQueue = taskQueue;
			m_running = running;
		}

		@Override
		public void run() {
			try {
				int batchSize = m_config.getBrokerSenderBatchSize();
				SendMessageCommandV5 cmd = m_taskQueue.pop(batchSize);

				if (cmd != null) {
					Context wholeSendProcessTimer = ProducerStatusMonitor.INSTANCE.getTimer(cmd.getTopic(),
					      cmd.getPartition(), "send-message-duration").time();

					if (!sendMessagesToBroker(cmd)) {
						m_taskQueue.push(cmd);
						tracking(cmd, false);
					} else {
						tracking(cmd, true);
					}

					wholeSendProcessTimer.stop();
				}
			} finally {
				m_running.set(false);
			}

		}

		private void tracking(SendMessageCommandV5 sendMessageCommand, boolean success) {
			if (!success || m_config.isCatEnabled()) {
				Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TRANSPORT,
				      sendMessageCommand.getTopic());
				t.addData("*count", sendMessageCommand.getMessageCount());
				t.setStatus(success ? Transaction.SUCCESS : "FAILED");
				t.complete();
			}

		}

		private boolean sendMessagesToBroker(SendMessageCommandV5 cmd) {
			String topic = cmd.getTopic();
			int partition = cmd.getPartition();
			try {
				Endpoint endpoint = m_endpointManager.getEndpoint(topic, partition);
				if (endpoint != null) {
					long correlationId = cmd.getHeader().getCorrelationId();

					Future<Pair<Boolean, Endpoint>> acceptFuture = m_messageAcceptanceMonitor.monitor(correlationId);
					Future<Boolean> resultFuture = m_messageResultMonitor.monitor(cmd);

					long timeout = m_config.getBrokerSenderSendTimeoutMillis();

					if (m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS)) {
						Context acceptTimer = ProducerStatusMonitor.INSTANCE.getTimer(topic, partition,
						      "broker-accept-duration").time();

						ProducerStatusMonitor.INSTANCE.wroteToBroker(topic, partition, cmd.getMessageCount());

						Pair<Boolean, Endpoint> acceptResult = waitForBrokerAcceptance(cmd, acceptFuture, timeout);

						acceptTimer.stop();

						if (acceptResult != null) {
							return waitForBrokerResultIfNecessary(cmd, resultFuture, acceptResult);
						} else {
							m_endpointManager.refreshEndpoint(topic, partition);
							return false;
						}
					} else {
						m_messageAcceptanceMonitor.cancel(correlationId);
						m_messageResultMonitor.cancel(cmd);
						return false;
					}
				} else {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("No endpoint found, ignore it");
					}
					m_endpointManager.refreshEndpoint(topic, partition);
					return false;
				}
			} catch (Exception e) {
				ProducerStatusMonitor.INSTANCE.sendFailed(topic, partition, cmd.getMessageCount());
				log.warn("Exception occurred while sending message to broker, will retry it");
				return false;
			}
		}

		private Pair<Boolean, Endpoint> waitForBrokerAcceptance(SendMessageCommandV5 cmd,
		      Future<Pair<Boolean, Endpoint>> acceptFuture, long timeout) throws InterruptedException, ExecutionException {
			Pair<Boolean, Endpoint> acceptResult = null;
			try {
				acceptResult = acceptFuture.get(timeout, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				ProducerStatusMonitor.INSTANCE.waitBrokerAcceptanceTimeout(cmd.getTopic(), cmd.getPartition(),
				      cmd.getMessageCount());
				m_messageAcceptanceMonitor.cancel(cmd.getHeader().getCorrelationId());
				m_messageResultMonitor.cancel(cmd);
			}
			return acceptResult;
		}

		private boolean waitForBrokerResultIfNecessary(SendMessageCommandV5 cmd, Future<Boolean> resultFuture,
		      Pair<Boolean, Endpoint> acceptResult) throws InterruptedException, ExecutionException {
			Boolean brokerAccept = acceptResult.getKey();
			String topic = cmd.getTopic();
			int partition = cmd.getPartition();
			if (brokerAccept != null && brokerAccept) {
				ProducerStatusMonitor.INSTANCE.brokerAccepted(topic, partition, cmd.getMessageCount());

				return waitForBrokerResult(cmd, resultFuture);
			} else {
				ProducerStatusMonitor.INSTANCE.brokerRejected(topic, partition, cmd.getMessageCount());
				Endpoint newEndpoint = acceptResult.getValue();
				if (newEndpoint != null) {
					m_endpointManager.updateEndpoint(topic, partition, newEndpoint);
				}
				return false;
			}
		}

		private boolean waitForBrokerResult(SendMessageCommandV5 cmd, Future<Boolean> resultFuture)
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

		private AtomicReference<SendMessageCommandV5> m_cmd = new AtomicReference<>(null);

		private BlockingQueue<ProducerWorkerContext> m_queue;

		public TaskQueue(String topic, int partition, int queueSize) {
			m_topic = topic;
			m_partition = partition;
			m_queue = new ArrayBlockingQueue<ProducerWorkerContext>(queueSize);

			ProducerStatusMonitor.INSTANCE.addTaskQueueGauge(m_topic, m_partition, m_queue);
		}

		public void push(SendMessageCommandV5 cmd) {
			m_cmd.set(cmd);
		}

		public SendMessageCommandV5 pop(int size) {
			if (m_cmd.get() == null) {
				return createSendMessageCommand(size);
			}
			return m_cmd.getAndSet(null);
		}

		private SendMessageCommandV5 createSendMessageCommand(int size) {
			SendMessageCommandV5 cmd = null;
			List<ProducerWorkerContext> contexts = new ArrayList<ProducerWorkerContext>(size);
			m_queue.drainTo(contexts, size);
			if (!contexts.isEmpty()) {
				cmd = new SendMessageCommandV5(m_topic, m_partition, m_config.getSendMessageReadResultTimeoutMillis());
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
