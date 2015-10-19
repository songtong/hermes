package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

public class StreamConsumingStrategy extends BaseConsumingStrategy {

	@Override
	protected ConsumerTask getConsumerTask(ConsumerContext context, int partitionId, int localCacheSize) {
		return new StreamConsumingStrategyConsumerTask(context, partitionId, localCacheSize);
	}

}
