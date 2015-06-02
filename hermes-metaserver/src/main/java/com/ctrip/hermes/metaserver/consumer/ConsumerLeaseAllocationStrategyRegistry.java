package com.ctrip.hermes.metaserver.consumer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerLeaseAllocationStrategyRegistry {

	public ConsumerLeaseAllocationStrategy findStrategy(String strategy);
}
