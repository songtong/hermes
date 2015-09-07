package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumingStrategy {

	SubscribeHandle start(ConsumerContext context, int partitionId);

}
