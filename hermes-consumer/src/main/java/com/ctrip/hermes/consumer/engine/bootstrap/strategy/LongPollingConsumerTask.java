package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LongPollingConsumerTask implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(LongPollingConsumerTask.class);

	private ConsumerNotifier m_consumerNotifier;

	private MessageCodec m_messageCodec;

	private EndpointManager m_endpointManager;

	private EndpointClient m_endpointClient;

	private LeaseManager<ConsumerLeaseKey> m_leaseManager;

	private SystemClockService m_systemClockService;

	private ConsumerConfig m_config;

	private ExecutorService m_pullMessageTaskExecutorService;

	private ScheduledExecutorService m_renewLeaseTaskExecutorService;

	private PullMessageResultMonitor m_pullMessageResultMonitor;

	private BlockingQueue<ConsumerMessage<?>> m_msgs;

	private int m_cacheSize;

	private int m_localCachePrefetchThreshold;

	private ConsumerContext m_context;

	private int m_partitionId;

	private AtomicBoolean m_pullTaskRunning = new AtomicBoolean(false);

	private AtomicReference<Lease> m_lease = new AtomicReference<>(null);

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	public LongPollingConsumerTask(ConsumerContext context, int partitionId, int cacheSize, int prefetchThreshold,
	      SystemClockService systemClockService) {
		m_context = context;
		m_partitionId = partitionId;
		m_cacheSize = cacheSize;
		m_localCachePrefetchThreshold = prefetchThreshold;
		m_msgs = new LinkedBlockingQueue<ConsumerMessage<?>>(m_cacheSize);
		m_systemClockService = systemClockService;

		m_pullMessageTaskExecutorService = Executors.newSingleThreadExecutor(HermesThreadFactory.create(String.format(
		      "LongPollingPullMessageTask-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId()), false));

		m_renewLeaseTaskExecutorService = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(String
		      .format("LongPollingRenewLeaseTask-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
		            m_context.getGroupId()), false));
	}

	public void setPullMessageResultMonitor(PullMessageResultMonitor pullMessageResultMonitor) {
		m_pullMessageResultMonitor = pullMessageResultMonitor;
	}

	public void setConfig(ConsumerConfig config) {
		m_config = config;
	}

	public void setSystemClockService(SystemClockService systemClockService) {
		m_systemClockService = systemClockService;
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

	public void setEndpointClient(EndpointClient endpointClient) {
		m_endpointClient = endpointClient;
	}

	public void setLeaseManager(LeaseManager<ConsumerLeaseKey> leaseManager) {
		m_leaseManager = leaseManager;
	}

	private boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public void run() {
		log.info("Consumer started(topic={}, partition={}, groupId={}, sessionId={})", m_context.getTopic().getName(),
		      m_partitionId, m_context.getGroupId(), m_context.getSessionId());
		ConsumerLeaseKey key = new ConsumerLeaseKey(new Tpg(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId()), m_context.getSessionId());
		while (!isClosed() && !Thread.currentThread().isInterrupted()) {
			try {
				acquireLease(key);

				if (!isClosed() && m_lease.get() != null && !m_lease.get().isExpired()) {
					long correlationId = CorrelationIdGenerator.generateCorrelationId();
					log.info(
					      "Consumer continue consuming(topic={}, partition={}, groupId={}, correlationId={}, sessionId={}), since lease acquired",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), correlationId,
					      m_context.getSessionId());

					startConsumingMessages(key, correlationId);

					log.info(
					      "Consumer pause consuming(topic={}, partition={}, groupId={}, correlationId={}, sessionId={}), since lease expired",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), correlationId,
					      m_context.getSessionId());
				}
			} catch (Exception e) {
				log.error("Exception occurred in consumer's run method(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}

		m_pullMessageTaskExecutorService.shutdown();
		m_renewLeaseTaskExecutorService.shutdown();
		log.info("Consumer stopped(topic={}, partition={}, groupId={}, sessionId={})", m_context.getTopic().getName(),
		      m_partitionId, m_context.getGroupId(), m_context.getSessionId());
	}

	private void startConsumingMessages(ConsumerLeaseKey key, long correlationId) {
		m_consumerNotifier.register(correlationId, m_context);

		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {

			try {
				// if leaseRemainingTime < stopConsumerTimeMillsBeforLeaseExpired, stop
				if (m_lease.get().getRemainingTime() <= m_config.getStopConsumerTimeMillsBeforLeaseExpired()) {
					if (log.isDebugEnabled()) {
						log.debug(
						      "Consumer pre-pause(topic={}, partition={}, groupId={}, correlationId={}, sessionId={}), since lease will be expired soon",
						      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), correlationId,
						      m_context.getSessionId());
					}
					break;
				}

				if (m_msgs.size() <= m_localCachePrefetchThreshold) {
					schedulePullMessagesTask(correlationId);
				}

				if (!m_msgs.isEmpty()) {
					consumeMessages(correlationId, m_cacheSize);
				} else {
					TimeUnit.MILLISECONDS.sleep(m_config.getNoMessageWaitIntervalMillis());
				}

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				log.error("Exception occurred while consuming message(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}

		// consume all remaining messages
		if (!m_msgs.isEmpty()) {
			consumeMessages(correlationId, 0);
		}

		m_consumerNotifier.deregister(correlationId);
		m_lease.set(null);
	}

	private void scheduleRenewLeaseTask(final ConsumerLeaseKey key, long delay) {
		m_renewLeaseTaskExecutorService.schedule(new Runnable() {

			@Override
			public void run() {
				if (isClosed()) {
					return;
				}

				Lease lease = m_lease.get();
				if (lease != null) {
					if (lease.getRemainingTime() > 0) {
						LeaseAcquireResponse response = m_leaseManager.tryRenewLease(key, lease);
						if (response != null && response.isAcquired()) {
							lease.setExpireTime(response.getLease().getExpireTime());
							scheduleRenewLeaseTask(key,
							      lease.getRemainingTime() - m_config.getRenewLeaseTimeMillisBeforeExpired());
							if (log.isDebugEnabled()) {
								log.debug("Consumer renew lease success(topic={}, partition={}, groupId={}, sessionId={})",
								      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
								      m_context.getSessionId());
							}
						} else {
							if (response != null && response.getNextTryTime() > 0) {
								scheduleRenewLeaseTask(key, response.getNextTryTime() - m_systemClockService.now());
							} else {
								scheduleRenewLeaseTask(key, m_config.getDefaultLeaseRenewDelayMillis());
							}

							if (log.isDebugEnabled()) {
								log.debug(
								      "Unable to renew consumer lease(topic={}, partition={}, groupId={}, sessionId={}), ignore it",
								      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
								      m_context.getSessionId());
							}
						}
					}
				}
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

	private void acquireLease(ConsumerLeaseKey key) {
		long nextTryTime = m_systemClockService.now();
		while (!isClosed() && !Thread.currentThread().isInterrupted()) {
			try {
				while (true) {
					if (!isClosed() && !Thread.currentThread().isInterrupted()) {
						if (nextTryTime > m_systemClockService.now()) {
							LockSupport.parkUntil(nextTryTime);
						} else {
							break;
						}
					} else {
						return;
					}
				}

				if (isClosed()) {
					return;
				}

				LeaseAcquireResponse response = m_leaseManager.tryAcquireLease(key);

				if (response != null && response.isAcquired() && !response.getLease().isExpired()) {
					m_lease.set(response.getLease());
					scheduleRenewLeaseTask(key,
					      m_lease.get().getRemainingTime() - m_config.getRenewLeaseTimeMillisBeforeExpired());

					if (log.isDebugEnabled()) {
						log.debug(
						      "Acquire consumer lease success(topic={}, partition={}, groupId={}, sessionId={}, leaseId={}, expireTime={})",
						      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
						      m_context.getSessionId(), response.getLease().getId(), new Date(response.getLease()
						            .getExpireTime()));
					}
					return;
				} else {
					if (response != null && response.getNextTryTime() > 0) {
						nextTryTime = response.getNextTryTime();
					} else {
						nextTryTime = m_systemClockService.now() + m_config.getDefaultLeaseAcquireDelayMillis();
					}

					if (log.isDebugEnabled()) {
						log.debug(
						      "Unable to acquire consumer lease(topic={}, partition={}, groupId={}, sessionId={}), ignore it",
						      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId());
					}
				}
			} catch (Exception e) {
				log.error("Exception occurred while acquiring lease(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}
	}

	private void consumeMessages(long correlationId, int maxItems) {
		List<ConsumerMessage<?>> msgs = new ArrayList<>(maxItems <= 0 ? 100 : maxItems);

		if (maxItems <= 0) {
			m_msgs.drainTo(msgs);
		} else {
			m_msgs.drainTo(msgs, maxItems);
		}

		m_consumerNotifier.messageReceived(correlationId, msgs);
	}

	@SuppressWarnings("rawtypes")
	private List<ConsumerMessage<?>> decodeBatches(List<TppConsumerMessageBatch> batches, Class bodyClazz,
	      Channel channel) {
		List<ConsumerMessage<?>> msgs = new ArrayList<>();
		for (TppConsumerMessageBatch batch : batches) {
			List<MessageMeta> msgMetas = batch.getMessageMetas();
			ByteBuf batchData = batch.getData();

			int partition = batch.getPartition();

			for (int j = 0; j < msgMetas.size(); j++) {
				BaseConsumerMessage baseMsg = m_messageCodec.decode(batch.getTopic(), batchData, bodyClazz);
				BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
				MessageMeta messageMeta = msgMetas.get(j);
				brokerMsg.setPartition(partition);
				brokerMsg.setPriority(messageMeta.getPriority() == 0 ? true : false);
				brokerMsg.setResend(messageMeta.isResend());
				brokerMsg.setChannel(channel);
				brokerMsg.setMsgSeq(messageMeta.getId());

				msgs.add(brokerMsg);
			}
		}

		return msgs;
	}

	private void schedulePullMessagesTask(long correlationId) {
		if (!isClosed() && m_pullTaskRunning.compareAndSet(false, true)) {
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
				if (isClosed() || m_msgs.size() > m_localCachePrefetchThreshold) {
					return;
				}

				Endpoint endpoint = m_endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);

				if (endpoint == null) {
					log.warn("No endpoint found for topic {} partition {}, will retry later",
					      m_context.getTopic().getName(), m_partitionId);
					TimeUnit.MILLISECONDS.sleep(m_config.getNoEndpointWaitIntervalMillis());
					return;
				}

				final SettableFuture<PullMessageResultCommand> future = SettableFuture.create();

				Lease lease = m_lease.get();
				if (lease != null) {
					long timeout = lease.getRemainingTime();

					if (timeout > 0) {
						PullMessageCommand cmd = new PullMessageCommand(m_context.getTopic().getName(), m_partitionId,
						      m_context.getGroupId(), m_cacheSize - m_msgs.size(), m_systemClockService.now() + timeout
						            - 500L);

						cmd.getHeader().setCorrelationId(m_correlationId);
						cmd.setFuture(future);

						PullMessageResultCommand ack = null;

						try {
							m_pullMessageResultMonitor.monitor(cmd);
							m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);

							ack = future.get(timeout, TimeUnit.MILLISECONDS);

							if (ack == null) {
								return;
							}
							List<TppConsumerMessageBatch> batches = ack.getBatches();
							if (batches != null && !batches.isEmpty()) {
								ConsumerContext context = m_consumerNotifier.find(m_correlationId);
								if (context != null) {
									Class<?> bodyClazz = context.getMessageClazz();

									List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz, ack.getChannel());
									m_msgs.addAll(msgs);
								} else {
									log.info(
									      "Can not find consumerContext(topic={}, partition={}, groupId={}, sessionId={}), maybe has been stopped.",
									      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
									      m_context.getSessionId());
								}
							}
						} finally {
							if (ack != null) {
								ack.release();
							}
						}
					}
				}
			} catch (TimeoutException e) {
				// ignore
			} catch (Exception e) {
				log.warn("Exception occurred while pulling message(topic={}, partition={}, groupId={}, sessionId={}).",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			} finally {
				m_pullTaskRunning.set(false);
			}
		}

	}

	public void close() {
		m_closed.set(true);
	}
}
