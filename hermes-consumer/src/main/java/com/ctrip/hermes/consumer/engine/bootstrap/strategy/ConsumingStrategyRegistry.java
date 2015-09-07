package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.ConsumerType;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumingStrategyRegistry {

	public ConsumingStrategy findStrategy(ConsumerType consumerType);
}
