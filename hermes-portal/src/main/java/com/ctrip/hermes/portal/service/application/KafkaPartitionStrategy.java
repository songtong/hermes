package com.ctrip.hermes.portal.service.application;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.topic.TopicView;

@Named(type=PartitionStrategy.class, value="kafka")
public class KafkaPartitionStrategy extends PartitionStrategy {
	public static final String DEFAULT_READ_DATASOURCE = "kafka-consumer";
	public static final String DEFAULT_WRITE_DATASOURCE = "kafka-producer";

	@Override
	protected void applyStrategy(TopicApplication application,
			TopicView topicView) {
		List<Property> kafkaProperties = new ArrayList<>();
		kafkaProperties.add(new Property("partitions").setValue("3"));
		kafkaProperties.add(new Property("replication-factor").setValue("2"));
		kafkaProperties.add(new Property("retention.ms")
				.setValue(String.valueOf(TimeUnit.DAYS.toMillis(application.getRetentionDays()))));
		
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

}
