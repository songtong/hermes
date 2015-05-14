package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageSender.class, value = Endpoint.BROKER)
public class BatchableMessageSender extends AbstractMessageSender implements MessageSender {

	private ConcurrentMap<Pair<String, Integer>, EndpointWritingWorkerThread> m_workers = new ConcurrentHashMap<>();

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {

		return createWorkerIfNecessary(msg.getTopic(), msg.getPartitionNo()).submit(msg);
	}

	private EndpointWritingWorkerThread createWorkerIfNecessary(String topic, int partition) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		if (!m_workers.containsKey(key)) {
			synchronized (m_workers) {
				if (!m_workers.containsKey(key)) {
					EndpointWritingWorkerThread worker = new EndpointWritingWorkerThread(topic, partition,
					      m_endpointManager, m_clientEndpointChannelManager);

					worker.setDaemon(true);
					worker.setName(String.format("ProducerChannelWorkerThread-Channel-%s-%s", topic, partition));
					worker.start();

					m_workers.put(key, worker);
				}
			}
		}
		return m_workers.get(key);
	}

	/**
	 * 
	 * @author Leo Liang(jhliang@ctrip.com)
	 *
	 */
	private static class EndpointWritingWorkerThread extends Thread {

		private BlockingQueue<ProducerChannelWorkerContext> m_queue = new LinkedBlockingQueue<>();

		private EndpointManager m_endpointManager;

		private ClientEndpointChannelManager m_clientEndpointChannelManager;

		private String m_topic;

		private int m_partition;

		private static final int BATCH_SIZE = 3000;

		private static final int INTERVAL_MILLISECONDS = 50;

		public EndpointWritingWorkerThread(String topic, int partition, EndpointManager endpointManager,
		      ClientEndpointChannelManager endpointChannelManager) {
			m_topic = topic;
			m_partition = partition;
			m_endpointManager = endpointManager;
			m_clientEndpointChannelManager = endpointChannelManager;
		}

		public Future<SendResult> submit(ProducerMessage<?> msg) {
			SettableFuture<SendResult> future = SettableFuture.create();

			m_queue.offer(new ProducerChannelWorkerContext(msg, future));

			return future;
		}

		@Override
		public void run() {
			while (!Thread.interrupted()) {
				try {
					List<ProducerChannelWorkerContext> contexts = new ArrayList<>();
					m_queue.drainTo(contexts, BATCH_SIZE);

					if (!contexts.isEmpty()) {
						SendMessageCommand command = new SendMessageCommand();
						for (ProducerChannelWorkerContext context : contexts) {
							command.addMessage(context.m_msg, context.m_future);
						}

						Endpoint endpoint = m_endpointManager.getEndpoint(m_topic, m_partition);
						EndpointChannel channel = m_clientEndpointChannelManager.getChannel(endpoint);

						channel.writeCommand(command);

					}

					TimeUnit.MILLISECONDS.sleep(INTERVAL_MILLISECONDS);

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {

				}
			}
		}

		private static class ProducerChannelWorkerContext {
			private ProducerMessage<?> m_msg;

			private SettableFuture<SendResult> m_future;

			/**
			 * @param msg
			 * @param future
			 */
			public ProducerChannelWorkerContext(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
				m_msg = msg;
				m_future = future;
			}

		}

	}

}
