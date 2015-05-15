package com.ctrip.hermes.producer.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.message.partition.PartitioningStrategy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.DefaultProducer;
import com.ctrip.hermes.producer.HermesProducerModule;
import com.ctrip.hermes.producer.pipeline.DefaultProducerPipelineSink;
import com.ctrip.hermes.producer.pipeline.DefaultProducerPipelineSinkManager;
import com.ctrip.hermes.producer.pipeline.EnrichMessageValve;
import com.ctrip.hermes.producer.pipeline.ProducerPipeline;
import com.ctrip.hermes.producer.pipeline.ProducerValveRegistry;
import com.ctrip.hermes.producer.pipeline.TracingMessageValve;
import com.ctrip.hermes.producer.sender.BatchableMessageSender;
import com.ctrip.hermes.producer.sender.MessageSender;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(HermesProducerModule.class));

		all.add(A(DefaultProducer.class));
		all.add(A(ProducerPipeline.class));
		all.add(A(ProducerValveRegistry.class));

		// valves
		all.add(A(TracingMessageValve.class));
		all.add(A(EnrichMessageValve.class));

		// sinks
		all.add(A(DefaultProducerPipelineSinkManager.class));
		all.add(C(PipelineSink.class, Endpoint.BROKER, DefaultProducerPipelineSink.class) //
		      .req(MessageSender.class, Endpoint.BROKER)//
		);

		// message sender
		all.add(C(MessageSender.class, Endpoint.BROKER, BatchableMessageSender.class)//
		      .req(EndpointManager.class)//
		      .req(ClientEndpointChannelManager.class)//
		      .req(PartitioningStrategy.class)//
		      .req(MetaService.class)//
		);
		all.add(C(MessageSender.class, Endpoint.TRANSACTION, BatchableMessageSender.class)//
		      .req(EndpointManager.class)//
		      .req(ClientEndpointChannelManager.class)//
		      .req(PartitioningStrategy.class)//
		      .req(MetaService.class)//
		);

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
