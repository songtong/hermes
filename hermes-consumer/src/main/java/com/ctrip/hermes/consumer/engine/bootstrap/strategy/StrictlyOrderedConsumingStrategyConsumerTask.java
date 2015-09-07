package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StrictlyOrderedConsumingStrategyConsumerTask extends BaseConsumerTask {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumingStrategyConsumerTask.class);

	private AtomicReference<PullMessagesTask> m_pullMessagesTask = new AtomicReference<>(null);

	private PullMessageResultMonitor m_pullMessageResultMonitor;

	public StrictlyOrderedConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int cacheSize) {
		super(context, partitionId, cacheSize);
		m_pullMessageResultMonitor = PlexusComponentLocator.lookup(PullMessageResultMonitor.class);
	}

	@Override
	protected void doBeforeConsuming(ConsumerLeaseKey key, long correlationId) {
		SchedulePolicy noEndpointSchedulePolicy = new ExponentialSchedulePolicy(m_config.getNoEndpointWaitBaseMillis(),
		      m_config.getNoEndpointWaitMaxMillis());
		m_pullMessagesTask.set(new PullMessagesTask(correlationId, noEndpointSchedulePolicy));
	}

	@Override
	protected void doAfterConsuming(ConsumerLeaseKey key, long correlationId) {
		m_pullMessagesTask.set(null);
	}

	protected Runnable getPullMessageTask() {
		return m_pullMessagesTask.get();
	}

	private class PullMessagesTask implements Runnable {
		private long m_correlationId;

		private SchedulePolicy m_noEndpointSchedulePolicy;

		public PullMessagesTask(long correlationId, SchedulePolicy noEndpointSchedulePolicy) {
			m_correlationId = correlationId;
			m_noEndpointSchedulePolicy = noEndpointSchedulePolicy;
		}

		@Override
		public void run() {
			try {
				if (isClosed() || !m_msgs.isEmpty()) {
					return;
				}

				Endpoint endpoint = m_endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);

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

		private void pullMessages(Endpoint endpoint, long timeout) throws InterruptedException, TimeoutException,
		      ExecutionException {
			final SettableFuture<PullMessageResultCommand> future = SettableFuture.create();

			PullMessageCommand cmd = new PullMessageCommand(m_context.getTopic().getName(), m_partitionId,
			      m_context.getGroupId(), m_msgs.remainingCapacity(), m_systemClockService.now() + timeout
			            + m_config.getPullMessageBrokerExpireTimeAdjustmentMills());

			cmd.getHeader().setCorrelationId(m_correlationId);
			cmd.setFuture(future);

			PullMessageResultCommand ack = null;

			try {

				Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_context.getTopic().getName(), m_partitionId,
				      m_context.getGroupId(), "pull-msg-cmd-duration");

				Context context = timer.time();

				m_pullMessageResultMonitor.monitor(cmd);
				m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);

				ConsumerStatusMonitor.INSTANCE.pullMessageCmdSent(m_context.getTopic().getName(), m_partitionId,
				      m_context.getGroupId());

				try {
					ack = future.get(timeout, TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					ConsumerStatusMonitor.INSTANCE.pullMessageCmdResultReadTimeout(m_context.getTopic().getName(),
					      m_partitionId, m_context.getGroupId());
					m_pullMessageResultMonitor.remove(cmd);
				}

				context.stop();

				if (ack != null) {
					ConsumerStatusMonitor.INSTANCE.pullMessageCmdResultReceived(m_context.getTopic().getName(),
					      m_partitionId, m_context.getGroupId());
					appendToMsgQueue(ack);
				}

			} finally {
				if (ack != null) {
					ack.release();
				}
			}
		}

		private void appendToMsgQueue(PullMessageResultCommand ack) {
			List<TppConsumerMessageBatch> batches = ack.getBatches();
			if (batches != null && !batches.isEmpty()) {
				ConsumerContext context = m_consumerNotifier.find(m_correlationId);
				if (context != null) {
					Class<?> bodyClazz = context.getMessageClazz();

					List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz, ack.getChannel());
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
