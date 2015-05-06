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
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.LeaseManager.LeaseAcquisitionListener;
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
	private LeaseManager<Tpg> m_leaseManager;

	@Inject
	private ConsumerNotifier m_consumerNotifier;

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private ClientEndpointChannelManager m_clientEndpointChannelManager;

	@Inject
	private MessageCodec m_messageCodec;

	// TODO
	private static final long MIN_POLL_TIMEOUT = 1000L;

	@Override
	public void start(ConsumerContext context, int partitionId) {
		m_leaseManager.registerAcquisition(new Tpg(context.getTopic().getName(), partitionId, context.getGroupId()),
		      new LongPollingConsumerLeaseAcquisitionListener(context, partitionId));

	}

	private class LongPollingConsumerLeaseAcquisitionListener implements LeaseAcquisitionListener {

		private ExecutorService m_consumerTaskThreadPool;

		private AtomicReference<ConsumerTask> m_consumerTask = new AtomicReference<>(null);

		private AtomicReference<Long> m_correlationId = new AtomicReference<>(null);

		private ConsumerContext m_context;

		private int m_partitionId;

		public LongPollingConsumerLeaseAcquisitionListener(ConsumerContext consumerContext, int partitionId) {
			m_context = consumerContext;
			m_partitionId = partitionId;
			// TODO thread factory
			m_consumerTaskThreadPool = Executors.newSingleThreadExecutor(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "ConsumerLongPollingThread");
				}
			});
		}

		@Override
		public void onExpire() {
			// TODO
			System.out.println(String.format("Lease expired...(topic=%s, partition=%s, consumerGroupId=%s)", m_context
			      .getTopic().getName(), m_partitionId, m_context.getGroupId()));
			ConsumerTask oldThread = m_consumerTask.getAndSet(null);
			if (oldThread != null) {
				oldThread.close();
			}
			m_correlationId.set(null);
		}

		@Override
		public void onAcquire(Lease lease) {

			m_correlationId.set(CorrelationIdGenerator.generateCorrelationId());
			// TODO
			System.out.println(String.format(
			      "Lease acquired...(topic=%s, partition=%s, consumerGroupId=%s, correlationId=%s)", m_context.getTopic()
			            .getName(), m_partitionId, m_context.getGroupId(), m_correlationId.get()));

			m_consumerTask.set(new ConsumerTask(m_context, m_partitionId, m_correlationId.get(), lease));
			m_consumerTaskThreadPool.submit(m_consumerTask.get());
		}
	}

	private class ConsumerTask implements Runnable {

		private ConsumerContext m_context;

		private int m_partitionId;

		private long m_correlationId;

		// TODO size configable dynamic
		private int m_cacheSize = 50;

		private Lease m_lease;

		private AtomicBoolean m_pullTaskRunning = new AtomicBoolean(false);

		private AtomicBoolean closed = new AtomicBoolean(false);

		private ExecutorService m_executorService;

		private BlockingQueue<ConsumerMessage<?>> m_msgs = new LinkedBlockingQueue<ConsumerMessage<?>>(m_cacheSize);

		public ConsumerTask(ConsumerContext context, int partitionId, long correlationId, Lease lease) {
			m_context = context;
			m_partitionId = partitionId;
			m_correlationId = correlationId;
			m_lease = lease;

			// TODO ThreadFactory
			m_executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, String.format("PullTask-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId()));
				}
			});
		}

		public void close() {
			closed.set(true);
		}

		@Override
		public void run() {
			// TODO
			System.out.println(String.format(
			      "Start long polling...(topic=%s, partition=%s, consumerGroupId=%s, correlationId=%s)", m_context
			            .getTopic().getName(), m_partitionId, m_context.getGroupId(), m_correlationId));

			m_consumerNotifier.register(m_correlationId, m_context);

			while (!closed.get() && !Thread.currentThread().isInterrupted()) {
				try {
					// TODO config size
					if (m_msgs.size() < 10) {
						schedulePullMessageTask();
					}

					if (!m_msgs.isEmpty()) {
						// TODO size
						consumeMessages(50);
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

			m_executorService.shutdown();

			try {
				m_executorService.awaitTermination(MIN_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// consume all remaining messages
			if (!m_msgs.isEmpty()) {
				consumeMessages(0);
			}

			// TODO
			System.out.println(String.format(
			      "Stop long polling...(topic=%s, partition=%s, consumerGroupId=%s, correlationId=%s)", m_context
			            .getTopic().getName(), m_partitionId, m_context.getGroupId(), m_correlationId));

			m_consumerNotifier.deregister(m_correlationId);

		}

		private void consumeMessages(int maxItems) {
			List<ConsumerMessage<?>> msgs = new ArrayList<>();

			if (maxItems <= 0) {
				m_msgs.drainTo(msgs);
			} else {
				m_msgs.drainTo(msgs, maxItems);
			}

			m_consumerNotifier.messageReceived(m_correlationId, msgs);
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

		private void schedulePullMessageTask() {
			if (m_pullTaskRunning.compareAndSet(false, true)) {
				m_executorService.submit(new PullMessageTask());
			}
		}

		private class PullMessageTask implements Runnable {

			@Override
			public void run() {
				try {
					Endpoint endpoint = m_endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);
					ClientEndpointChannel channel = m_clientEndpointChannelManager.getChannel(endpoint);

					final SettableFuture<PullMessageAckCommand> future = SettableFuture.create();

					long now = System.currentTimeMillis();

					long timeout = m_lease.getExpireTime() - now;

					// TODO if lease.expTime < MIN_POLL_TIMEOUT
					if (timeout < MIN_POLL_TIMEOUT && timeout > 0) {
						timeout = MIN_POLL_TIMEOUT;
					}

					if (timeout > 0) {
						PullMessageCommand cmd = new PullMessageCommand(m_context.getTopic().getName(), m_partitionId,
						      m_context.getGroupId(), m_cacheSize - m_msgs.size(), now + timeout);

						cmd.getHeader().setCorrelationId(m_correlationId);
						cmd.setFuture(future);

						channel.writeCommand(cmd);

						PullMessageAckCommand ack = future.get(timeout, TimeUnit.MILLISECONDS);
						try {
							if (ack == null) {
								return;
							}
							List<TppConsumerMessageBatch> batches = ack.getBatches();
							Class<?> bodyClazz = m_consumerNotifier.find(m_correlationId).getMessageClazz();

							List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz, channel);
							m_msgs.addAll(msgs);
						} finally {
							if (ack != null) {
								ack.release();
							}
						}
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				} finally {
					m_pullTaskRunning.set(false);
				}
			}

		}
	}
}
