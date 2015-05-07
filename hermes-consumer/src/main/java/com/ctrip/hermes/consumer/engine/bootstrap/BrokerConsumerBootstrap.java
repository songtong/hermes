package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
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
	protected SubscribeHandle doStart(final ConsumerContext context) {

		List<Partition> partitions = m_metaService.getPartitions(context.getTopic().getName(), context.getGroupId());
		for (final Partition partition : partitions) {
			m_consumptionStrategyRegistry.findStrategy(context.getConsumerType()).start(context, partition.getId());
		}

		// TODO
		return null;
	}
}
