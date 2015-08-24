package com.ctrip.hermes.consumer.engine.bootstrap;

import com.ctrip.hermes.meta.entity.Topic;



/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerBootstrapManager {

	public ConsumerBootstrap findConsumerBootStrap(Topic topic);

}
