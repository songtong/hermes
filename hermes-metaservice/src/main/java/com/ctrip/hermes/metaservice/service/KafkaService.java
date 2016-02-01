package com.ctrip.hermes.metaservice.service;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

@Named
public class KafkaService {

	@Inject
	private StorageService storageService;

	public void resetToLatest(String topic, String consumerGroup) {
		Properties prop = getProperties(consumerGroup);
		KafkaConsumer consumer = new KafkaConsumer(prop);
		consumer.subscribe(Arrays.asList(topic));
		Set<TopicPartition> assignment = consumer.assignment();
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		if (assignment.size() != partitions.size()) {
			System.out.println("ASSIGNMENTS: " + assignment);
			System.out.println("PARTITIONS: " + partitions);
			System.out.println("Could not match, reset latest failed");
			return;
		}
		for (TopicPartition tp : assignment) {
			consumer.seekToEnd(tp);
		}
		consumer.commitSync();
		consumer.close();
	}

	public void resetToEarliest(String topic, String consumerGroup) {
		Properties prop = getProperties(consumerGroup);
		KafkaConsumer consumer = new KafkaConsumer(prop);
		consumer.subscribe(Arrays.asList(topic));
		Set<TopicPartition> assignment = consumer.assignment();
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		if (assignment.size() != partitions.size()) {
			System.out.println("ASSIGNMENTS: " + assignment);
			System.out.println("PARTITIONS: " + partitions);
			System.out.println("Could not match, reset earliest failed");
			return;
		}
		for (TopicPartition tp : assignment) {
			consumer.seekToBeginning(tp);
		}
		consumer.commitSync();
		consumer.close();
	}

	private Properties getProperties(String consumerGroup) {
		Properties props = new Properties();
		String brokerList = storageService.getKafkaBrokerList();
		props.put("metadata.broker.list", brokerList);
		props.put("group.id", consumerGroup);
		props.put("auto.commit.enable", "false");
		return props;
	}
}
