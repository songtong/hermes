package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.transport.command.PullMessageAckCommand;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerLongPollingConsumptionStrategy implements BrokerConsumptionStrategy {
	@Inject
	private ConsumerNotifier m_consumerNotifier;

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private ClientEndpointChannelManager m_clientEndpointChannelManager;

	@Inject
	private MessageCodec m_messageCodec;

	@Override
	public void start(ConsumerContext consumerContext, int partitionId) {
		// TODO
		System.out.println(String.format(
		      "Start long polling consumer bootstrap(topic=%s, partition=%s, consumerGroupId=%s)", consumerContext
		            .getTopic().getName(), partitionId, consumerContext.getGroupId()));

		final long correlationId = CorrelationIdGenerator.generateCorrelationId();

		m_consumerNotifier.register(correlationId, consumerContext);

		// TODO cache it
		Thread consumerThread = newConsumerThread(consumerContext, partitionId, correlationId);
		consumerThread.start();
	}

	private Thread newConsumerThread(final ConsumerContext consumerContext, final int partitionId,
	      final long correlationId) {
		Thread consumerThread = new Thread(new ConsumerTask(consumerContext, partitionId, correlationId));
		return consumerThread;
	}

	private class ConsumerTask implements Runnable {

		private ConsumerContext m_context;

		private int m_partitionId;

		private long m_correlationId;

		// TODO size configable dynamic
		private int m_cacheSize = 50;

		private BlockingQueue<ConsumerMessage<?>> m_msgs = new LinkedBlockingQueue<ConsumerMessage<?>>(m_cacheSize);

		private AtomicBoolean m_pullTaskRunning = new AtomicBoolean(false);

		// TODO
		private ExecutorService m_executorService;

		public ConsumerTask(ConsumerContext context, int partitionId, long correlationId) {
			m_context = context;
			m_partitionId = partitionId;
			m_correlationId = correlationId;

			// TODO
			m_executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, String.format("PullTask-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId()));
				}
			});
		}

		@Override
		public void run() {

			while (!Thread.currentThread().isInterrupted()) {
				try {
					// TODO config size
					if (m_msgs.size() < 10) {
						schedulePullTask(m_context, m_partitionId, m_correlationId);
					}

					if (!m_msgs.isEmpty()) {
						List<ConsumerMessage<?>> msgs = new ArrayList<>();

						// TODO size
						m_msgs.drainTo(msgs, 50);

						m_consumerNotifier.messageReceived(m_correlationId, msgs);
					} else {
						// TODO
						Thread.sleep(10);
					}

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					// TODO
				}
			}

		}

		@SuppressWarnings("rawtypes")
		private List<ConsumerMessage<?>> decodeBatches(List<TppConsumerMessageBatch> batches, Class bodyClazz,
		      EndpointChannel channel) {
			List<ConsumerMessage<?>> msgs = new ArrayList<>();
			for (TppConsumerMessageBatch batch : batches) {
				List<Pair<Long, Integer>> msgSeqs = batch.getMsgSeqs();
				ByteBuf batchData = batch.getData();

				int partition = batch.getPartition();
				boolean priority = batch.isPriority();

				for (int j = 0; j < msgSeqs.size(); j++) {
					BaseConsumerMessage baseMsg = m_messageCodec.decode(batch.getTopic(), batchData, bodyClazz);
					BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
					brokerMsg.setPartition(partition);
					brokerMsg.setPriority(priority);
					brokerMsg.setResend(batch.isResend());
					brokerMsg.setChannel(channel);
					brokerMsg.setMsgSeq(msgSeqs.get(j).getKey());

					msgs.add(brokerMsg);
				}
			}

			return msgs;
		}

		private void schedulePullTask(final ConsumerContext consumerContext, final int partitionId,
		      final long correlationId) {
			if (m_pullTaskRunning.compareAndSet(false, true)) {
				// TODO
				System.out.println(String.format("Schedule pull task(topic=%s, partitionId=%s, consumerGroupId=%s)",
				      consumerContext.getTopic().getName(), partitionId, consumerContext.getGroupId()));
				m_executorService.submit(new Runnable() {

					@Override
					public void run() {
						try {
							Endpoint endpoint = m_endpointManager.getEndpoint(consumerContext.getTopic().getName(),
							      partitionId);
							ClientEndpointChannel channel = m_clientEndpointChannelManager.getChannel(endpoint);

							final SettableFuture<PullMessageAckCommand> future = SettableFuture.create();

							// TODO set exp time
							PullMessageCommand cmd = new PullMessageCommand(consumerContext.getTopic().getName(), partitionId,
							      consumerContext.getGroupId(), m_cacheSize - m_msgs.size(),
							      System.currentTimeMillis() + 10 * 1000L);

							cmd.getHeader().setCorrelationId(correlationId);
							cmd.setFuture(future);

							channel.writeCommand(cmd);

							// TODO timeout?
							// TODO use exp time same as cmd?
							PullMessageAckCommand ack = future.get(10, TimeUnit.SECONDS);
							try {
								if (ack == null) {
									return;
								}
								List<TppConsumerMessageBatch> batches = ack.getBatches();
								Class<?> bodyClazz = m_consumerNotifier.find(correlationId).getMessageClazz();

								List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz, channel);
								m_msgs.addAll(msgs);
							} finally {
								if (ack != null) {
									ack.release();
								}
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							// e.printStackTrace();
						} finally {
							m_pullTaskRunning.set(false);
						}
					}
				});
			}
		}
	}
}
