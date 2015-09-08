package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultConsumingStrategyConsumerTask extends BaseConsumerTask {

	private AtomicReference<PullMessagesTask> m_pullMessagesTask = new AtomicReference<>(null);

	public DefaultConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int cacheSize) {
		super(context, partitionId, cacheSize);
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

	@Override
	protected BrokerConsumerMessage<?> decorateBrokerMessage(BrokerConsumerMessage<?> brokerMsg) {
		brokerMsg.setAckWithForwardOnly(false);
		return brokerMsg;
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
			// do nothing
		}

		@Override
		protected PullMessageCommandV2 createPullMessageCommand(long timeout) {
			return new PullMessageCommandV2(PullMessageCommandV2.PULL_WITHOUT_OFFSET, m_context.getTopic().getName(),
			      m_partitionId, m_context.getGroupId(), null, m_msgs.remainingCapacity(), m_systemClockService.now()
			            + timeout + m_config.getPullMessageBrokerExpireTimeAdjustmentMills());
		}

	}

}
