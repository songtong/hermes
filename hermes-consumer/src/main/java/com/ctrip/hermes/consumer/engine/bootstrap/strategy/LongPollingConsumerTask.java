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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
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
public class LongPollingConsumerTask implements Runnable {

	private ConsumerNotifier m_consumerNotifier;

	private MessageCodec m_messageCodec;

	private EndpointManager m_endpointManager;

	private ClientEndpointChannelManager m_clientEndpointChannelManager;

	private LeaseManager<ConsumerLeaseKey> m_leaseManager;

	private ExecutorService m_pullMessageTaskExecutorService;

	private ExecutorService m_renewLeaseTaskExecutorService;

	private BlockingQueue<ConsumerMessage<?>> m_msgs;

	private int m_cacheSize;

	private long m_renewLeaseTimeMillisBeforeExpired;

	private long m_stopConsumerTimeMillsBeforLeaseExpired;

	private ConsumerContext m_context;

	private int m_partitionId;

	private AtomicBoolean m_pullTaskRunning = new AtomicBoolean(false);

	private AtomicBoolean m_renewLeaseTaskRunning = new AtomicBoolean(false);

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	private AtomicReference<Lease> m_lease = new AtomicReference<>(null);

	private AtomicLong m_nextRenewLeaseTime = new AtomicLong(0);

	public LongPollingConsumerTask(ConsumerContext context, int partitionId, int cacheSize,
	      long renewLeaseTimeMillisBeforeExpired, long stopConsumerTimeMillsBeforLeaseExpired) {
		m_context = context;
		m_partitionId = partitionId;
		m_cacheSize = cacheSize;
		m_msgs = new LinkedBlockingQueue<ConsumerMessage<?>>(m_cacheSize);
		m_renewLeaseTimeMillisBeforeExpired = renewLeaseTimeMillisBeforeExpired;
		m_stopConsumerTimeMillsBeforLeaseExpired = stopConsumerTimeMillsBeforLeaseExpired;

		// TODO ThreadFactory
		m_pullMessageTaskExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("LongPollingPullMessageTask-%s-%s-%s", m_context.getTopic().getName(),
				      m_partitionId, m_context.getGroupId()));
			}
		});

		// TODO ThreadFactory
		m_renewLeaseTaskExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("LongPollingRenewLeaseTask-%s-%s-%s", m_context.getTopic().getName(),
				      m_partitionId, m_context.getGroupId()));
			}
		});
	}

	public void setConsumerNotifier(ConsumerNotifier consumerNotifier) {
		m_consumerNotifier = consumerNotifier;
	}

	public void setMessageCodec(MessageCodec messageCodec) {
		m_messageCodec = messageCodec;
	}

	public void setEndpointManager(EndpointManager endpointManager) {
		m_endpointManager = endpointManager;
	}

	public void setClientEndpointChannelManager(ClientEndpointChannelManager clientEndpointChannelManager) {
		m_clientEndpointChannelManager = clientEndpointChannelManager;
	}

	public void setLeaseManager(LeaseManager<ConsumerLeaseKey> leaseManager) {
		m_leaseManager = leaseManager;
	}

	public void close() {
		m_closed.set(true);
	}

	@Override
	public void run() {
		ConsumerLeaseKey key = new ConsumerLeaseKey(new Tpg(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId()), m_context.getSessionId());
		while (!m_closed.get() && !Thread.currentThread().isInterrupted()) {
			try {
				acquireLease(key);

				if (m_lease.get() != null && m_lease.get().getExpireTime() > System.currentTimeMillis()) {
					long correlationId = CorrelationIdGenerator.generateCorrelationId();
					// TODO
					System.out.println(String.format(
					      "Lease acquired...(topic=%s, partition=%s, consumerGroupId=%s, correlationId=%s, sessionId=%s)",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), correlationId,
					      m_context.getSessionId()));
					startConsumer(key, correlationId);

					// TODO
					System.out.println(String.format(
					      "Lease expired...(topic=%s, partition=%s, consumerGroupId=%s, correlationId=%s, sessionId=%s)",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), correlationId,
					      m_context.getSessionId()));
				}
			} catch (Exception e) {
				// TODO
				e.printStackTrace();
			}
		}

	}

	private void startConsumer(ConsumerLeaseKey key, long correlationId) {
		m_nextRenewLeaseTime.set(System.currentTimeMillis());
		m_consumerNotifier.register(correlationId, m_context);

		while (!m_closed.get() //
		      && !Thread.currentThread().isInterrupted()//
		      && m_lease.get().getExpireTime() > System.currentTimeMillis()) {

			try {
				long leaseRemainingTime = m_lease.get().getExpireTime() - System.currentTimeMillis();

				// if leaseRemainingTime < stopConsumerTimeMillsBeforLeaseExpired, stop
				if (leaseRemainingTime <= m_stopConsumerTimeMillsBeforLeaseExpired) {
					break;
				}

				if (leaseRemainingTime <= m_renewLeaseTimeMillisBeforeExpired
				      && m_nextRenewLeaseTime.get() <= System.currentTimeMillis()) {
					scheduleRenewLeaseTask(key);
				}

				// TODO config size
				if (m_msgs.size() < 10) {
					schedulePullMessagesTask(correlationId);
				}

				if (!m_msgs.isEmpty()) {
					consumeMessages(correlationId, m_cacheSize);
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

		// consume all remaining messages
		if (!m_msgs.isEmpty()) {
			consumeMessages(correlationId, 0);
		}

		m_consumerNotifier.deregister(correlationId);
		m_lease.set(null);
	}

	private void scheduleRenewLeaseTask(final ConsumerLeaseKey key) {
		if (m_renewLeaseTaskRunning.compareAndSet(false, true)) {
			m_renewLeaseTaskExecutorService.submit(new Runnable() {

				@Override
				public void run() {
					try {
						Lease lease = m_lease.get();
						if (lease != null) {
							LeaseAcquireResponse response = m_leaseManager.tryRenewLease(key, lease);
							if (response != null && response.isAcquired()) {
								lease.setExpireTime(response.getLease().getExpireTime());
							} else {
								if (response != null && response.getNextTryTime() > 0) {
									m_nextRenewLeaseTime.set(response.getNextTryTime());
								} else {
									// TODO configable delay
									m_nextRenewLeaseTime.set(System.currentTimeMillis() + 500L);
								}
							}
						}
					} finally {
						m_renewLeaseTaskRunning.set(false);
					}
				}
			});
		}
	}

	private void acquireLease(ConsumerLeaseKey key) {
		long nextTryTime = System.currentTimeMillis();
		while (!m_closed.get() && !Thread.currentThread().isInterrupted()) {
			try {
				while (true) {
					if (!m_closed.get() && !Thread.currentThread().isInterrupted()) {
						if (nextTryTime > System.currentTimeMillis()) {
							LockSupport.parkUntil(nextTryTime);
						} else {
							break;
						}
					} else {
						return;
					}
				}

				LeaseAcquireResponse response = m_leaseManager.tryAcquireLease(key);

				if (response != null && response.isAcquired()) {
					m_lease.set(response.getLease());
					return;
				} else {
					nextTryTime = response.getNextTryTime();
				}
			} catch (Exception e) {
				// TODO
				e.printStackTrace();
			}
		}
	}

	private void consumeMessages(long correlationId, int maxItems) {
		List<ConsumerMessage<?>> msgs = new ArrayList<>();

		if (maxItems <= 0) {
			m_msgs.drainTo(msgs);
		} else {
			m_msgs.drainTo(msgs, maxItems);
		}

		m_consumerNotifier.messageReceived(correlationId, msgs);
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

	private void schedulePullMessagesTask(long correlationId) {
		if (m_pullTaskRunning.compareAndSet(false, true)) {
			// TODO
			System.out.println("Pull Messages...");
			m_pullMessageTaskExecutorService.submit(new PullMessagesTask(correlationId));
		}
	}

	private class PullMessagesTask implements Runnable {
		private long m_correlationId;

		public PullMessagesTask(long correlationId) {
			m_correlationId = correlationId;
		}

		@Override
		public void run() {
			try {
				Endpoint endpoint = m_endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);
				ClientEndpointChannel channel = m_clientEndpointChannelManager.getChannel(endpoint);

				final SettableFuture<PullMessageAckCommand> future = SettableFuture.create();

				long now = System.currentTimeMillis();

				Lease lease = m_lease.get();
				if (lease != null) {
					long timeout = lease.getExpireTime() - now;

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
