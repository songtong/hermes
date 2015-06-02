package com.ctrip.hermes.metaserver.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseAllocationStrategyRegistry.class)
public class DefaultConsumerLeaseAllocationStrategyRegistry extends ContainerHolder implements Initializable,
      ConsumerLeaseAllocationStrategyRegistry {

	private Map<String, ConsumerLeaseAllocationStrategy> m_strategies = new ConcurrentHashMap<>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, ConsumerLeaseAllocationStrategy> strategies = lookupMap(ConsumerLeaseAllocationStrategy.class);

		for (Map.Entry<String, ConsumerLeaseAllocationStrategy> entry : strategies.entrySet()) {
			m_strategies.put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public ConsumerLeaseAllocationStrategy findStrategy(String strategy) {
		return m_strategies.get(strategy);
	}

}
