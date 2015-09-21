package com.ctrip.hermes.consumer.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.consumer.DefaultConsumer;
import com.ctrip.hermes.consumer.engine.DefaultEngine;
import com.ctrip.hermes.consumer.engine.bootstrap.BrokerConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.DefaultConsumerBootstrapManager;
import com.ctrip.hermes.consumer.engine.bootstrap.DefaultConsumerBootstrapRegistry;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.ConsumingStrategy;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.DefaultConsumingRegistry;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.DefaultConsumingStrategy;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.StrictlyOrderedConsumingStrategy;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.consumer.pipeline.internal.ConsumerAuditValve;
import com.ctrip.hermes.consumer.engine.consumer.pipeline.internal.ConsumerTracingValve;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager;
import com.ctrip.hermes.consumer.engine.monitor.DefaultPullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.monitor.DefaultQueryOffsetResultMonitor;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.consumer.engine.notifier.DefaultConsumerNotifier;
import com.ctrip.hermes.consumer.engine.pipeline.ConsumerPipeline;
import com.ctrip.hermes.consumer.engine.pipeline.ConsumerValveRegistry;
import com.ctrip.hermes.consumer.engine.pipeline.DefaultConsumerPipelineSink;
import com.ctrip.hermes.consumer.engine.transport.command.processor.PullMessageResultCommandProcessor;
import com.ctrip.hermes.consumer.engine.transport.command.processor.QueryOffsetResultCommandProcessor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// consumer
		all.add(A(DefaultConsumer.class));

		// engine
		all.add(A(DefaultEngine.class));

		// bootstrap
		all.add(A(DefaultConsumerBootstrapManager.class));
		all.add(A(DefaultConsumerBootstrapRegistry.class));
		all.add(A(BrokerConsumerBootstrap.class));

		// consuming strategy
		all.add(A(DefaultConsumingRegistry.class));
		all.add(C(ConsumingStrategy.class, ConsumerType.DEFAULT.toString(), DefaultConsumingStrategy.class)//
		      .req(ConsumerConfig.class)//
		);
		all.add(C(ConsumingStrategy.class, ConsumerType.STRICTLY_ORDERING.toString(),
		      StrictlyOrderedConsumingStrategy.class)//
		      .req(ConsumerConfig.class)//
		);

		all.add(A(DefaultConsumerPipelineSink.class));

		all.add(C(CommandProcessor.class, CommandType.RESULT_MESSAGE_PULL_V2.toString(),
		      PullMessageResultCommandProcessor.class)//
		      .req(PullMessageResultMonitor.class));
		all.add(C(CommandProcessor.class, CommandType.RESULT_QUERY_OFFSET.toString(),
		      QueryOffsetResultCommandProcessor.class)//
		      .req(QueryOffsetResultMonitor.class));

		all.add(A(DefaultPullMessageResultMonitor.class));
		all.add(A(DefaultQueryOffsetResultMonitor.class));

		// notifier
		all.add(A(DefaultConsumerNotifier.class));
		all.add(A(ConsumerValveRegistry.class));

		all.add(A(ConsumerTracingValve.class));
		all.add(A(ConsumerAuditValve.class));

		all.add(A(ConsumerPipeline.class));

		all.add(A(ConsumerLeaseManager.class));

		all.add(A(ConsumerConfig.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
