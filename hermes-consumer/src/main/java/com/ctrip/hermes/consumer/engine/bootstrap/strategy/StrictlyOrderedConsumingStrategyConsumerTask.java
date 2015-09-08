package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

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
import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.command.QueryOffsetCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;
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

	private QueryOffsetResultMonitor m_queryOffsetResultMonitor;

	private SchedulePolicy m_noEndpointSchedulePolicy;

	private AtomicReference<Offset> m_offset = new AtomicReference<>(null);

	public StrictlyOrderedConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int cacheSize) {
		super(context, partitionId, cacheSize);
		m_queryOffsetResultMonitor = PlexusComponentLocator.lookup(QueryOffsetResultMonitor.class);
	}

	@Override
	protected void doBeforeConsuming(ConsumerLeaseKey key, long correlationId) {
		m_noEndpointSchedulePolicy = new ExponentialSchedulePolicy(m_config.getNoEndpointWaitBaseMillis(),
		      m_config.getNoEndpointWaitMaxMillis());

		queryLatestOffset(key, correlationId);
		// reset policy
		m_noEndpointSchedulePolicy.succeess();

		m_pullMessagesTask.set(new PullMessagesTask(correlationId, m_noEndpointSchedulePolicy));
	}

	@Override
	protected BrokerConsumerMessage<?> decorateBrokerMessage(BrokerConsumerMessage<?> brokerMsg) {
		brokerMsg.setAckWithForwardOnly(true);
		return brokerMsg;
	}

	private void queryLatestOffset(ConsumerLeaseKey key, long correlationId) {

		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {

			Endpoint endpoint = m_endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);
			if (endpoint == null) {
				log.warn("No endpoint found for topic {} partition {}, will retry later", m_context.getTopic().getName(),
				      m_partitionId);
				m_noEndpointSchedulePolicy.fail(true);
				continue;
			} else {
				m_noEndpointSchedulePolicy.succeess();
			}

			final SettableFuture<QueryOffsetResultCommand> future = SettableFuture.create();

			QueryOffsetCommand cmd = new QueryOffsetCommand(m_context.getTopic().getName(), m_partitionId,
			      m_context.getGroupId());

			cmd.getHeader().setCorrelationId(correlationId);
			cmd.setFuture(future);

			QueryOffsetResultCommand offsetRes = null;

			Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_context.getTopic().getName(), m_partitionId,
			      m_context.getGroupId(), "query-offset-cmd-duration");

			Context context = timer.time();

			long timeout = m_config.getQueryOffsetTimeoutMillis();

			m_queryOffsetResultMonitor.monitor(cmd);
			m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);

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
				m_queryOffsetResultMonitor.remove(cmd);
			}

			context.stop();

			if (offsetRes != null && offsetRes.getOffset() != null) {
				ConsumerStatusMonitor.INSTANCE.queryOffsetCmdResultReceived(m_context.getTopic().getName(), m_partitionId,
				      m_context.getGroupId());
				m_offset.set(offsetRes.getOffset());
				return;
			} else {
				m_noEndpointSchedulePolicy.fail(true);
			}

		}
	}

	@Override
	protected void doAfterConsuming(ConsumerLeaseKey key, long correlationId) {
		m_pullMessagesTask.set(null);
		m_offset.set(null);
	}

	@Override
	protected Runnable getPullMessageTask() {
		return m_pullMessagesTask.get();
	}

	private class PullMessagesTask extends BasePullMessagesTask {

		public PullMessagesTask(long correlationId, SchedulePolicy noEndpointSchedulePolicy) {
			super(correlationId, noEndpointSchedulePolicy);
		}

		@Override
		protected void resultReceived(PullMessageResultCommandV2 ack) {
			if (ack.getOffset() != null) {
				m_offset.set(ack.getOffset());
			}
		}

		@Override
		protected PullMessageCommandV2 createPullMessageCommand(long timeout) {
			return new PullMessageCommandV2(PullMessageCommandV2.PULL_WITH_OFFSET, m_context.getTopic().getName(),
			      m_partitionId, m_context.getGroupId(), m_offset.get(), m_msgs.remainingCapacity(),
			      m_systemClockService.now() + timeout + m_config.getPullMessageBrokerExpireTimeAdjustmentMills());
		}

	}

}
