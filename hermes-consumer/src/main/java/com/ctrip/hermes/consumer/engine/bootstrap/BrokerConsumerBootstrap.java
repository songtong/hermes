package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.BrokerConsumptionStrategyRegistry;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerBootstrap.class, value = Endpoint.BROKER)
public class BrokerConsumerBootstrap extends BaseConsumerBootstrap {

	@Inject
	private BrokerConsumptionStrategyRegistry m_consumptionStrategyRegistry;

	@Override
	protected void doStart(final ConsumerContext consumerContext) {

		List<Partition> partitions = m_metaService.getPartitions(consumerContext.getTopic().getName(),
		      consumerContext.getGroupId());

		for (final Partition partition : partitions) {

			m_consumptionStrategyRegistry.findStrategy(consumerContext.getConsumerType()).start(consumerContext,
			      partition.getId());

		}

	}

}
