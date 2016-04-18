package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
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

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.FilterMessageListenerConfig;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.transport.command.v3.QueryLatestConsumerOffsetCommandV3;
import com.ctrip.hermes.core.transport.command.v3.QueryOffsetResultCommandV3;
import com.ctrip.hermes.core.transport.command.v4.PullMessageCommandV4;
import com.ctrip.hermes.core.transport.command.v4.PullMessageResultCommandV4;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseConsumerTask implements ConsumerTask {

	private static final Logger log = LoggerFactory.getLogger(BaseConsumerTask.class);

	protected SystemClockService m_systemClockService;

	protected LeaseManager<ConsumerLeaseKey> m_leaseManager;

	protected ConsumerConfig m_config;

	protected ConsumerNotifier m_consumerNotifier;

	protected ExecutorService m_pullMessageTaskExecutor;

	protected ScheduledExecutorService m_renewLeaseTaskExecutor;

	protected BlockingQueue<ConsumerMessage<?>> m_msgs;

	protected ConsumerContext m_context;

	protected int m_partitionId;

	protected AtomicBoolean m_pullTaskRunning = new AtomicBoolean(false);

	protected AtomicReference<Lease> m_lease = new AtomicReference<Lease>(null);

	protected AtomicBoolean m_closed = new AtomicBoolean(false);

	protected RetryPolicy m_retryPolicy;

	protected AtomicReference<Runnable> m_pullMessagesTask = new AtomicReference<>(null);

	protected AtomicReference<Offset> m_offset = new AtomicReference<>(null);

	protected AckManager m_ackManager;

	protected int m_maxAckHolderSize;

	public BaseConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, AckManager ackManager) {
		this(context, partitionId, localCacheSize, 0, ackManager);
	}

	public BaseConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, int maxAckHolderSize) {
		this(context, partitionId, localCacheSize, maxAckHolderSize, PlexusComponentLocator.lookup(AckManager.class));
	}

	@SuppressWarnings("unchecked")
	private BaseConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, int maxAckHolderSize,
	      AckManager ackManager) {
		m_context = context;
		m_partitionId = partitionId;
		m_msgs = new LinkedBlockingQueue<ConsumerMessage<?>>(localCacheSize);
		m_maxAckHolderSize = maxAckHolderSize;

		m_leaseManager = PlexusComponentLocator.lookup(LeaseManager.class, BuildConstants.CONSUMER);
		m_consumerNotifier = PlexusComponentLocator.lookup(ConsumerNotifier.class);
		m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);
		m_config = PlexusComponentLocator.lookup(ConsumerConfig.class);
		m_retryPolicy = PlexusComponentLocator.lookup(MetaService.class).findRetryPolicyByTopicAndGroup(
		      context.getTopic().getName(), context.getGroupId());
		m_ackManager = ackManager;

		m_pullMessageTaskExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create(
		      String.format("PullMessageThread-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
		            m_context.getGroupId()), false));

		m_renewLeaseTaskExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      String.format("RenewLeaseThread-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
		            m_context.getGroupId()), false));

		ConsumerStatusMonitor.INSTANCE.addMessageQueueGuage(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId(), m_msgs);
	}

	protected boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public void start() {
		// FIXME one boss thread to acquire lease, if acquired, run this task
		log.info("Consumer started(mode={}, topic={}, partition={}, groupId={}, sessionId={})",
		      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
		      m_context.getSessionId());

		ConsumerLeaseKey key = new ConsumerLeaseKey(new Tpg(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId()), m_context.getSessionId());

		while (!isClosed() && !Thread.currentThread().isInterrupted()) {
			try {
				acquireLease(key);

				if (!isClosed() && m_lease.get() != null && !m_lease.get().isExpired()) {

					long token = CorrelationIdGenerator.generateCorrelationId();

					log.info(
					      "Consumer continue consuming(mode={}, topic={}, partition={}, groupId={}, token={}, sessionId={}), since lease acquired",
					      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId(), token, m_context.getSessionId());

					startConsuming(key, token);

					log.info(
					      "Consumer pause consuming(mode={}, topic={}, partition={}, groupId={}, token={}, sessionId={}), since lease expired",
					      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId(), token, m_context.getSessionId());
				}
			} catch (Exception e) {
				log.error("Exception occurred in consumer's run method(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}

		stopConsumer();
	}

	protected void stopConsumer() {
		m_pullMessageTaskExecutor.shutdown();
		m_renewLeaseTaskExecutor.shutdown();

		ConsumerStatusMonitor.INSTANCE.removeMonitor(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId());

		log.info("Consumer stopped(mode={}, topic={}, partition={}, groupId={}, sessionId={})",
		      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
		      m_context.getSessionId());
	}

	protected void startConsuming(ConsumerLeaseKey key, long token) {
		m_consumerNotifier.register(token, m_context);
		m_ackManager.register(token, new Tpg(m_context.getTopic().getName(), m_partitionId, m_context.getGroupId()),
		      m_maxAckHolderSize);
		doBeforeConsuming(key, token);

		m_msgs.clear();

		SchedulePolicy noMessageSchedulePolicy = new ExponentialSchedulePolicy(m_config.getNoMessageWaitBaseMillis(),
		      m_config.getNoMessageWaitMaxMillis());

		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {

			try {
				// if leaseRemainingTime < stopConsumerTimeMillsBeforLeaseExpired, stop
				if (m_lease.get().getRemainingTime() <= m_config.getStopConsumerTimeMillsBeforLeaseExpired()) {

					if (log.isDebugEnabled()) {
						log.debug(
						      "Consumer pre-pause(topic={}, partition={}, groupId={}, token={}, sessionId={}), since lease will be expired soon",
						      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), token,
						      m_context.getSessionId());
					}
					break;
				}

				if (m_msgs.isEmpty()) {
					schedulePullMessagesTask();
				}

				if (!m_msgs.isEmpty()) {
					consumeMessages(token);
					noMessageSchedulePolicy.succeess();
				} else {
					noMessageSchedulePolicy.fail(true);
				}

			} catch (Exception e) {
				log.error("Exception occurred while consuming message(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}

		m_consumerNotifier.deregister(token);
		m_ackManager.deregister(token);
		m_lease.set(null);
		doAfterConsuming(key);
	}

	protected void schedulePullMessagesTask() {
		if (!isClosed() && m_pullTaskRunning.compareAndSet(false, true)) {
			m_pullMessageTaskExecutor.submit(getPullMessageTask());
		}
	}

	protected Runnable getPullMessageTask() {
		return m_pullMessagesTask.get();
	}

	protected Runnable createPullMessageTask(long token, SchedulePolicy noEndpointSchedulePolicy) {
		return new BasePullMessagesTask(token, noEndpointSchedulePolicy);
	}

	protected void doBeforeConsuming(ConsumerLeaseKey key, long token) {
		queryLatestOffset(key);

		SchedulePolicy noEndpointSchedulePolicy = new ExponentialSchedulePolicy(m_config.getNoEndpointWaitBaseMillis(),
		      m_config.getNoEndpointWaitMaxMillis());
		m_pullMessagesTask.set(createPullMessageTask(token, noEndpointSchedulePolicy));
	}

	protected void queryLatestOffset(ConsumerLeaseKey key) {

		SchedulePolicy schedulePolicy = new ExponentialSchedulePolicy(m_config.getNoEndpointWaitBaseMillis(),
		      m_config.getNoEndpointWaitMaxMillis());

		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {

			Endpoint endpoint = PlexusComponentLocator.lookup(EndpointManager.class).getEndpoint(
			      m_context.getTopic().getName(), m_partitionId);
			if (endpoint == null) {
				log.warn("No endpoint found for topic {} partition {}, will retry later", m_context.getTopic().getName(),
				      m_partitionId);
				schedulePolicy.fail(true);
			} else {
				final SettableFuture<QueryOffsetResultCommandV3> future = SettableFuture.create();

				QueryLatestConsumerOffsetCommandV3 cmd = new QueryLatestConsumerOffsetCommandV3(m_context.getTopic()
				      .getName(), m_partitionId, m_context.getGroupId());

				cmd.setFuture(future);

				QueryOffsetResultCommandV3 offsetRes = null;

				Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_context.getTopic().getName(), m_partitionId,
				      m_context.getGroupId(), "query-offset-cmd-duration");

				Context context = timer.time();

				long timeout = m_config.getQueryOffsetTimeoutMillis();

				QueryOffsetResultMonitor queryOffsetResultMonitor = PlexusComponentLocator
				      .lookup(QueryOffsetResultMonitor.class);

				queryOffsetResultMonitor.monitor(cmd);
				if (PlexusComponentLocator.lookup(EndpointClient.class).writeCommand(endpoint, cmd, timeout,
				      TimeUnit.MILLISECONDS)) {

					ConsumerStatusMonitor.INSTANCE.queryOffsetCmdSent(m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId());

					try {
						offsetRes = future.get(timeout, TimeUnit.MILLISECONDS);
					} catch (TimeoutException e) {
						ConsumerStatusMonitor.INSTANCE.queryOffsetCmdResultReadTimeout(m_context.getTopic().getName(),
						      m_partitionId, m_context.getGroupId());
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} catch (ExecutionException e) {
						ConsumerStatusMonitor.INSTANCE.queryOffsetCmdError(m_context.getTopic().getName(), m_partitionId,
						      m_context.getGroupId());
					} finally {
						queryOffsetResultMonitor.remove(cmd);
					}
				} else {
					queryOffsetResultMonitor.remove(cmd);
				}

				context.stop();

				if (offsetRes != null && offsetRes.getOffset() != null) {
					ConsumerStatusMonitor.INSTANCE.queryOffsetCmdResultReceived(m_context.getTopic().getName(),
					      m_partitionId, m_context.getGroupId());
					m_offset.set(offsetRes.getOffset());
					return;
				} else {
					schedulePolicy.fail(true);
				}
			}
		}
	}

	protected void doAfterConsuming(ConsumerLeaseKey key) {
		m_pullMessagesTask.set(null);
		m_offset.set(null);
	}

	protected void scheduleRenewLeaseTask(final ConsumerLeaseKey key, long delay) {
		m_renewLeaseTaskExecutor.schedule(new Runnable() {

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

	protected void acquireLease(ConsumerLeaseKey key) {
		long nextTryTime = m_systemClockService.now();
		while (!isClosed() && !Thread.currentThread().isInterrupted()) {
			try {
				waitForNextTryTime(nextTryTime);

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

	protected void waitForNextTryTime(long nextTryTime) {
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
	}

	protected void consumeMessages(long token) {
		List<ConsumerMessage<?>> msgs = new ArrayList<ConsumerMessage<?>>();

		m_msgs.drainTo(msgs);

		m_consumerNotifier.messageReceived(token, msgs);
		ConsumerStatusMonitor.INSTANCE.messageProcessed(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId(), msgs.size());
	}

	@SuppressWarnings("rawtypes")
	protected List<ConsumerMessage<?>> decodeBatches(List<TppConsumerMessageBatch> batches, Class bodyClazz) {
		try {
			List<ConsumerMessage<?>> msgs = new ArrayList<ConsumerMessage<?>>();
			for (TppConsumerMessageBatch batch : batches) {
				List<MessageMeta> msgMetas = batch.getMessageMetas();
				ByteBuf batchData = batch.getData();

				int partition = batch.getPartition();

				for (int j = 0; j < msgMetas.size(); j++) {
					Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId(), "decode-duration");
					Context context = timer.time();

					BaseConsumerMessage baseMsg = PlexusComponentLocator.lookup(MessageCodec.class).decode(batch.getTopic(),
					      batchData, bodyClazz);
					BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
					MessageMeta messageMeta = msgMetas.get(j);
					brokerMsg.setPartition(partition);
					brokerMsg.setPriority(messageMeta.getPriority() == 0 ? true : false);
					brokerMsg.setResend(messageMeta.isResend());
					brokerMsg.setRetryTimesOfRetryPolicy(m_retryPolicy.getRetryTimes());
					brokerMsg.setMsgSeq(messageMeta.getId());

					context.stop();

					msgs.add(decorateBrokerMessage(brokerMsg));
				}
			}

			return msgs;
		} catch (Exception e) {
			log.error("Failed to deserialize msg, because type mismatch between producer and consumer.", e);
			throw e;
		}
	}

	protected BrokerConsumerMessage<?> decorateBrokerMessage(BrokerConsumerMessage<?> brokerMsg) {
		return brokerMsg;
	}

	public void close() {
		m_closed.set(true);
	}

	protected class BasePullMessagesTask implements Runnable {
		protected long m_token;

		protected SchedulePolicy m_noEndpointSchedulePolicy;

		public BasePullMessagesTask(long token, SchedulePolicy noEndpointSchedulePolicy) {
			m_token = token;
			m_noEndpointSchedulePolicy = noEndpointSchedulePolicy;
		}

		@Override
		public void run() {
			try {
				if (isClosed() || !m_msgs.isEmpty()) {
					return;
				}

				Endpoint endpoint = PlexusComponentLocator.lookup(EndpointManager.class).getEndpoint(
				      m_context.getTopic().getName(), m_partitionId);

				if (endpoint == null) {
					log.warn("No endpoint found for topic {} partition {}, will retry later",
					      m_context.getTopic().getName(), m_partitionId);
					m_noEndpointSchedulePolicy.fail(true);
					return;
				} else {
					m_noEndpointSchedulePolicy.succeess();
				}

				Lease lease = m_lease.get();
				if (lease != null) {
					long timeout = lease.getRemainingTime();

					if (timeout > 0) {
						pullMessages(endpoint, timeout);
					}
				}
			} catch (Exception e) {
				log.warn("Exception occurred while pulling message(topic={}, partition={}, groupId={}, sessionId={}).",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			} finally {
				m_pullTaskRunning.set(false);
			}
		}

		protected void pullMessages(Endpoint endpoint, long timeout) throws InterruptedException, TimeoutException,
		      ExecutionException {
			final SettableFuture<PullMessageResultCommandV4> future = SettableFuture.create();

			PullMessageCommandV4 cmd = createPullMessageCommand(timeout);

			cmd.setFuture(future);

			PullMessageResultCommandV4 ack = null;

			try {

				Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_context.getTopic().getName(), m_partitionId,
				      m_context.getGroupId(), "pull-msg-cmd-duration");

				Context context = timer.time();

				PullMessageResultMonitor pullMessageResultMonitor = PlexusComponentLocator
				      .lookup(PullMessageResultMonitor.class);

				pullMessageResultMonitor.monitor(cmd);
				if (PlexusComponentLocator.lookup(EndpointClient.class).writeCommand(endpoint, cmd, timeout,
				      TimeUnit.MILLISECONDS)) {

					ConsumerStatusMonitor.INSTANCE.pullMessageCmdSent(m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId());

					try {
						ack = future.get(timeout, TimeUnit.MILLISECONDS);
					} catch (TimeoutException e) {
						ConsumerStatusMonitor.INSTANCE.pullMessageCmdResultReadTimeout(m_context.getTopic().getName(),
						      m_partitionId, m_context.getGroupId());
					} finally {
						pullMessageResultMonitor.remove(cmd);
					}

					context.stop();

					if (ack != null) {
						ConsumerStatusMonitor.INSTANCE.pullMessageCmdResultReceived(m_context.getTopic().getName(),
						      m_partitionId, m_context.getGroupId());
						appendToMsgQueue(ack);

						resultReceived(ack);
					}
				} else {
					pullMessageResultMonitor.remove(cmd);
				}

			} finally {
				if (ack != null) {
					ack.release();
				}
			}
		}

		protected void resultReceived(PullMessageResultCommandV4 ack) {
			if (ack.getOffset() != null) {
				m_offset.set(ack.getOffset());
			}
		}

		protected PullMessageCommandV4 createPullMessageCommand(long timeout) {
			return new PullMessageCommandV4( //
			      m_context.getTopic().getName(), //
			      m_partitionId, //
			      m_context.getGroupId(), //
			      m_offset.get(), //
			      m_msgs.remainingCapacity(), //
			      timeout, //
			      m_context.getMessageListenerConfig() instanceof FilterMessageListenerConfig ? //
			      ((FilterMessageListenerConfig) m_context.getMessageListenerConfig()).getFilter()
			            : null);
		}

		protected void appendToMsgQueue(PullMessageResultCommandV4 ack) {
			List<TppConsumerMessageBatch> batches = ack.getBatches();
			if (batches != null && !batches.isEmpty()) {
				ConsumerContext context = m_consumerNotifier.find(m_token);
				if (context != null) {
					Class<?> bodyClazz = context.getMessageClazz();

					List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz);

					for (ConsumerMessage<?> msg : msgs) {
						m_ackManager.delivered(m_token, msg);
					}

					m_msgs.addAll(msgs);

					ConsumerStatusMonitor.INSTANCE.messageReceived(m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId(), msgs.size());

				} else {
					log.info(
					      "Can not find consumerContext(topic={}, partition={}, groupId={}, sessionId={}), maybe has been stopped.",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId());
				}
			}
		}

	}
}
