package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

	private ConcurrentMap<Pair<String, Integer>, ProducerEndpointWorker> m_workers = new ConcurrentHashMap<>();

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {

		return createWorkerIfNecessary(msg.getTopic(), msg.getPartitionNo()).submit(msg);
	}

	private ProducerEndpointWorker createWorkerIfNecessary(String topic, int partition) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		if (!m_workers.containsKey(key)) {
			synchronized (m_workers) {
				if (!m_workers.containsKey(key)) {

					// TODO config batchSize, interval, timeout
					ProducerEndpointWorker worker = new ProducerEndpointWorker(topic, partition, 3000, 50L, 50L);

					worker.start();

					m_workers.put(key, worker);
				}
			}
		}
		return m_workers.get(key);
	}

	private class ProducerEndpointWorker extends Thread {

		private BlockingQueue<ProducerWorkerContext> m_queue = new LinkedBlockingQueue<>();

		private String m_topic;

		private int m_partition;

		private int m_batchSize;

		private long m_interval;

		private long m_timeout;

		public ProducerEndpointWorker(String topic, int partition, int batchSize, long interval, long timeout) {
			m_topic = topic;
			m_partition = partition;
			m_batchSize = batchSize;
			m_interval = interval;
			m_timeout = timeout;
			setDaemon(true);
			setName(String.format("ProducerEndpointWorker-%s-%s", topic, partition));
		}

		public Future<SendResult> submit(ProducerMessage<?> msg) {
			SettableFuture<SendResult> future = SettableFuture.create();

			m_queue.offer(new ProducerWorkerContext(msg, future));

			return future;
		}

		@Override
		public void run() {
			SendMessageCommand cmd = null;

			while (!Thread.interrupted()) {
				try {
					if (cmd == null) {
						cmd = createSendMessageCommand();
					}

					if (cmd != null && sendMessagesToBroker(cmd)) {
						cmd = null;
					}

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					// TODO
					e.printStackTrace();
				} finally {
					try {
						TimeUnit.MILLISECONDS.sleep(m_interval);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
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

		private SendMessageCommand createSendMessageCommand() {
			SendMessageCommand cmd = null;
			List<ProducerWorkerContext> contexts = new ArrayList<>();
			m_queue.drainTo(contexts, m_batchSize);
			if (!contexts.isEmpty()) {
				cmd = new SendMessageCommand(m_topic, m_partition);
				for (ProducerWorkerContext context : contexts) {
					cmd.addMessage(context.m_msg, context.m_future);
				}
			}
			return cmd;
		}

		private class ProducerWorkerContext {
			private ProducerMessage<?> m_msg;

			private SettableFuture<SendResult> m_future;

			public ProducerWorkerContext(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
				m_msg = msg;
				m_future = future;
			}

		}

	}

}
