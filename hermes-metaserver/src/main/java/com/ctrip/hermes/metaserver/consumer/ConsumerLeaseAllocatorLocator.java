package com.ctrip.hermes.metaserver.consumer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerLeaseAllocatorLocator {

	public ConsumerLeaseAllocator findAllocator(String topicName, String consumerGroupName);
}
