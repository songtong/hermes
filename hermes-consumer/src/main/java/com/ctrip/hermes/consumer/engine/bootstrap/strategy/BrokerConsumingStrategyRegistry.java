package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.ConsumerType;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerConsumingStrategyRegistry {

	public BrokerConsumingStrategy findStrategy(ConsumerType consumerType);
}
