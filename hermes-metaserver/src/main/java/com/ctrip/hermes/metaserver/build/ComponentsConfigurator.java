package com.ctrip.hermes.metaserver.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerLeaseAllocator;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerPartitionAssigningStrategy;
import com.ctrip.hermes.metaserver.cluster.ClusterStateChangeListenerContainer;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterTopicAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.listener.EventEngineBootstrapListener;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.consumer.ActiveConsumerListHolder;
import com.ctrip.hermes.metaserver.consumer.ConsumerAssignmentHolder;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;
import com.ctrip.hermes.metaserver.consumer.DefaultConsumerLeaseAllocatorLocator;
import com.ctrip.hermes.metaserver.consumer.DefaultOrderedConsumeConsumerPartitionAssigningStrategy;
import com.ctrip.hermes.metaserver.consumer.NonOrderedConsumeConsumerLeaseAllocator;
import com.ctrip.hermes.metaserver.consumer.OrderedConsumeConsumerLeaseAllocator;
import com.ctrip.hermes.metaserver.event.DefaultEventHandlerRegistry;
import com.ctrip.hermes.metaserver.event.impl.BaseMetaChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.BrokerLeaseChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.BrokerListChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.LeaderInitEventHandler;
import com.ctrip.hermes.metaserver.event.impl.MetaServerListChangedEventHandler;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaservice.service.SubscriptionService;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(MetaHolder.class));
		all.add(A(MetaServerConfig.class));

		// consumer lease
		all.add(A(OrderedConsumeConsumerLeaseAllocator.class));
		all.add(A(NonOrderedConsumeConsumerLeaseAllocator.class));
		all.add(A(ActiveConsumerListHolder.class));
		all.add(A(DefaultOrderedConsumeConsumerPartitionAssigningStrategy.class));
		all.add(A(ConsumerAssignmentHolder.class));
		all.add(A(ConsumerLeaseHolder.class));
		all.add(A(DefaultConsumerLeaseAllocatorLocator.class));

		// broker lease
		all.add(A(DefaultBrokerLeaseAllocator.class));
		all.add(A(DefaultBrokerPartitionAssigningStrategy.class));
		all.add(A(BrokerLeaseHolder.class));
		all.add(A(BrokerAssignmentHolder.class));

		// subscription service
		all.add(A(SubscriptionService.class));

		// cluster
		all.add(A(ClusterStateHolder.class));
		all.add(A(ClusterTopicAssignmentHolder.class));

		all.add(A(ClusterStateChangeListenerContainer.class));

		// event handler
		all.add(A(EventEngineBootstrapListener.class));
		all.add(A(DefaultEventHandlerRegistry.class));
		all.add(A(EndpointMaker.class));

		all.add(A(LeaderInitEventHandler.class));
		all.add(A(BaseMetaChangedEventHandler.class));
		all.add(A(BrokerListChangedEventHandler.class));
		all.add(A(MetaServerListChangedEventHandler.class));
		all.add(A(BrokerLeaseChangedEventHandler.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
