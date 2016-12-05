package com.ctrip.hermes.portal.service.application;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.topic.TopicView;

@Named(type = PartitionStrategy.class, value = "kafka")
public class KafkaPartitionStrategy extends PartitionStrategy {
	public static final String DEFAULT_READ_DATASOURCE = "kafka-consumer";

	public static final String DEFAULT_WRITE_DATASOURCE = "kafka-producer";

	public static final String DEFAULT_KAFKA_BROKER_GROUP = "kafka-default";

	@Override
	protected void applyStrategy(TopicApplication application, TopicView topicView) {
		topicView.setBrokerGroup(DEFAULT_KAFKA_BROKER_GROUP);

		List<Property> kafkaProperties = new ArrayList<>();
		kafkaProperties.add(new Property("partitions").setValue("3"));
		kafkaProperties.add(new Property("replication-factor").setValue("2"));
		kafkaProperties.add(new Property("retention.ms").setValue(String.valueOf(TimeUnit.DAYS.toMillis(application
		      .getRetentionDays()))));

		topicView.setProperties(kafkaProperties);
		if ("java".equals(application.getLanguageType())) {
			topicView.setEndpointType("kafka");
		} else if (".net".equals(application.getLanguageType())) {
			topicView.setEndpointType("broker");
		}
		super.applyStrategy(application, topicView);
	}

	@Override
	protected StrategyDatasource getDefaultDatasource(TopicApplication application) {
		return StrategyDatasource.newInstance(Pair.from(DEFAULT_READ_DATASOURCE, DEFAULT_WRITE_DATASOURCE));
	}

	@Override
	protected String getDefaultBrokerGroup(TopicApplication application) {
		Set<String> brokerGroups = new HashSet<>();
		for (Endpoint endpoint : m_endpointService.getEndpoints().values()) {
			brokerGroups.add(endpoint.getGroup());
		}

		String defaultBrokerGroup = "kafka-default";
		if (!brokerGroups.contains(defaultBrokerGroup)) {
			defaultBrokerGroup = "default";
		}

		return defaultBrokerGroup;
	}
}
