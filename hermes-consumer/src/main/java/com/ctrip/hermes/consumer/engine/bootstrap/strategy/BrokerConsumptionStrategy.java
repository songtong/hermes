package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerConsumptionStrategy {

	void start(ConsumerContext context, int partitionId);
}
