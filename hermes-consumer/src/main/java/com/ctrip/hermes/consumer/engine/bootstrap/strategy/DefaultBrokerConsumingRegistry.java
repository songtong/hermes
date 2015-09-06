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
@Named(type = BrokerConsumingStrategyRegistry.class)
public class DefaultBrokerConsumingRegistry extends ContainerHolder implements Initializable,
      BrokerConsumingStrategyRegistry {

	private Map<ConsumerType, BrokerConsumingStrategy> m_strategies = new ConcurrentHashMap<ConsumerType, BrokerConsumingStrategy>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, BrokerConsumingStrategy> strategies = lookupMap(BrokerConsumingStrategy.class);

		for (Map.Entry<String, BrokerConsumingStrategy> entry : strategies.entrySet()) {
			m_strategies.put(ConsumerType.valueOf(entry.getKey()), entry.getValue());
		}
	}

	@Override
	public BrokerConsumingStrategy findStrategy(ConsumerType consumerType) {
		return m_strategies.get(consumerType);
	}

}
