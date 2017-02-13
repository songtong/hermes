package com.ctrip.hermes.admin.core.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.kafka.KafkaConstants;

@Named
public class KafkaService {

	@Inject
	private StorageService storageService;

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaService.class);

	public Pair<Boolean, String> resetConsumerOffset(String topic, String consumerGroup, String position) {
		Pair<Boolean, String> resultInfo = new Pair<Boolean, String>(true, "");
		List<Properties> props = getProperties(topic, consumerGroup);
		for (Properties prop : props) {
			KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);
			List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
			List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();

			if (partitionInfos == null) {
				m_logger.warn("Topic not exists in kafka:{}",
				      prop.getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME));
				resultInfo.setKey(false);
				resultInfo.setValue(resultInfo.getValue()
				      + String.format("Reset failed in kafka:%s, topic not exists!",
				            prop.getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME)));
				continue;
			}

			for (PartitionInfo partitionInfo : partitionInfos) {
				TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
				topicPartitions.add(tp);
			}
			consumer.assign(topicPartitions);
			Set<TopicPartition> assignment = consumer.assignment();
			List<PartitionInfo> partitions = consumer.partitionsFor(topic);
			if (assignment.size() != partitions.size()) {
				m_logger.warn("ASSIGNMENTS: " + assignment);
				m_logger.warn("PARTITIONS: " + partitions);
				m_logger.warn("Could not match, reset failed");
				resultInfo.setKey(false);
				resultInfo.setValue(resultInfo.getValue()
				      + String.format("Reset failed in kafka:%s, assignments and partitions dismatch!",
				            prop.getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME)));
				consumer.close();
				continue;
			}

			try {
				for (TopicPartition tp : assignment) {
					long before = consumer.position(tp);
					if (position.equals(ConsumerService.RESET_OPTION_EARLIEST)) {
						consumer.seekToBeginning(tp);
					} else if (position.equals(ConsumerService.RESET_OPTION_LATEST)) {
						consumer.seekToEnd(tp);
					}
					long after = consumer.position(tp);
					m_logger.info("Reset partition: {} From: {} To: {}", tp.partition(), before, after);
				}
				consumer.commitSync();
				m_logger.info("Reset offset Done");
			} catch (Exception e) {
				resultInfo.setKey(false);
				resultInfo.setValue(resultInfo.getValue()
				      + String.format("Reset failed in kafka:%s, %s. Maybe consumer is still alive!",
				            prop.getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME), e.getMessage()));
			} finally {
				consumer.close();
			}
		}

		return resultInfo;
	}

	private List<Properties> getProperties(String topic, String consumerGroup) {
		List<Properties> configs = new ArrayList<>();

		Map<String, String> brokerList = storageService.getKafkaBrokerList();
		for (String brokers : brokerList.values()) {
			Properties config = new Properties();
			config.put("bootstrap.servers", brokers);
			config.put("group.id", consumerGroup);
			config.put("enable.auto.commit", "false");
			config.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
			config.put("key.deserializer", StringDeserializer.class.getCanonicalName());

			configs.add(config);
		}
		return configs;
	}

}