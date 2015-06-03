package com.ctrip.hermes.metaserver.consumer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerLeaseAllocatorLocator {

	public ConsumerLeaseAllocator findStrategy(String topicName, String consumerGroupName);
}
