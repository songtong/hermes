package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultConsumingStrategy extends BaseConsumingStrategy {

	@Override
	protected ConsumerTask getConsumerTask(ConsumerContext context, int partitionId, int localCacheSize) {
		return new DefaultConsumingStrategyConsumerTask(context, partitionId, localCacheSize);
	}

}
