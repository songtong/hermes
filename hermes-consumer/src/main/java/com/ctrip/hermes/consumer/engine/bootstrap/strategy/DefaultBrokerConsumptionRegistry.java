package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.ConsumerType;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerConsumptionStrategyRegistry.class)
public class DefaultBrokerConsumptionRegistry extends ContainerHolder implements Initializable,
      BrokerConsumptionStrategyRegistry {

	private Map<ConsumerType, BrokerConsumptionStrategy> m_strategies = new ConcurrentHashMap<ConsumerType, BrokerConsumptionStrategy>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, BrokerConsumptionStrategy> strategies = lookupMap(BrokerConsumptionStrategy.class);

		for (Map.Entry<String, BrokerConsumptionStrategy> entry : strategies.entrySet()) {
			m_strategies.put(ConsumerType.valueOf(entry.getKey()), entry.getValue());
		}
	}

	@Override
	public BrokerConsumptionStrategy findStrategy(ConsumerType consumerType) {
		return m_strategies.get(consumerType);
	}

}
