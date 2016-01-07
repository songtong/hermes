package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.MessageStreamOffset;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;

public class StreamConsumingStrategyConsumerTask extends StrictlyOrderedConsumingStrategyConsumerTask {
	private static final Logger log = LoggerFactory.getLogger(StreamConsumingStrategyConsumerTask.class);

	public StreamConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int localCacheSize,
	      int maxAckHolderSize) {
		super(context, partitionId, localCacheSize, maxAckHolderSize);
	}

	@Override
	protected void queryLatestOffset(ConsumerLeaseKey key, long correlationId) {
		SchedulePolicy queryOffsetSchedulePolicy = new ExponentialSchedulePolicy(
		      (int) m_config.getQueryOffsetTimeoutMillis() / 5, (int) m_config.getQueryOffsetTimeoutMillis());

		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {
			try {
				String topic = m_context.getTopic().getName();
				MessageStreamOffset offset = m_context.getOffsetStorage().queryLatestOffset(topic, m_partitionId);
				if (offset == null) {
					queryOffsetSchedulePolicy.fail(true);
					continue;
				}
				m_offset.set(new Offset(offset.getPriorityOffset(), offset.getNonPriorityOffset(), null));
				return;
			} catch (Exception e) {
				log.error("Query latest offset failed: {}:{}", m_context.getTopic().getName(), m_partitionId, e);
			}
		}
	}
}
