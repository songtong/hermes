package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.CompositeSubscribeHandle;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.BrokerConsumingStrategy;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.BrokerConsumingStrategyRegistry;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerBootstrap.class, value = Endpoint.BROKER)
public class BrokerConsumerBootstrap extends BaseConsumerBootstrap {

	@Inject
	private BrokerConsumingStrategyRegistry m_consumptionStrategyRegistry;

	@Override
	protected SubscribeHandle doStart(final ConsumerContext context) {

		CompositeSubscribeHandle handler = new CompositeSubscribeHandle();

		List<Partition> partitions = m_metaService.listPartitionsByTopic(context.getTopic().getName());
		BrokerConsumingStrategy consumingStrategy = m_consumptionStrategyRegistry.findStrategy(context.getConsumerType());
		for (final Partition partition : partitions) {
			handler.addSubscribeHandle(m_consumptionStrategyRegistry.findStrategy(context.getConsumerType()).start(
			      context, partition.getId()));
		}

		return handler;
	}
}
