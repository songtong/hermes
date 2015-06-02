package com.ctrip.hermes.metaserver.consumer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerLeaseAllocationStrategyLocator {

	public ConsumerLeaseAllocationStrategy findStrategy(String topicName, String consumerGroupName);
}
