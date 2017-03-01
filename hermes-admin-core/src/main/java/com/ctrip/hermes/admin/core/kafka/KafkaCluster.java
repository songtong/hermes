package com.ctrip.hermes.admin.core.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.hermes.admin.core.service.ConsumerService.ResetPosition;
import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Property;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaCluster {
	private String m_idc;

	private String m_bootstrapServers;

	private String m_zookeeperConnect;

	public String getIdc() {
		return m_idc;
	}

	public void setIdc(String idc) {
		m_idc = idc;
	}

	public String getBootstrapServers() {
		return m_bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		m_bootstrapServers = bootstrapServers;
	}

	public String getZookeeperConnect() {
		return m_zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		m_zookeeperConnect = zookeeperConnect;
	}

	public void resetOffset(String topic, String consumerGroup, ResetPosition resetOption) {
		KafkaConsumer<String, byte[]> consumer = simpleConsumer(topic, consumerGroup);
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
		List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();

		if (partitionInfos == null) {
			throw new RuntimeException("Topic not exist!");
		}

		for (PartitionInfo partitionInfo : partitionInfos) {
			TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
			topicPartitions.add(tp);
		}
		consumer.assign(topicPartitions);
		Set<TopicPartition> assignment = consumer.assignment();
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		if (assignment.size() != partitions.size()) {
			consumer.close();
			throw new RuntimeException(String.format("Assignments:%s and partitions:%s dismatch!", assignment, partitions));
		}

		try {
			for (TopicPartition tp : assignment) {
				if (ResetPosition.EARLIEST.equals(resetOption)) {
					consumer.seekToBeginning(tp);
				} else if (ResetPosition.LATEST.equals(resetOption)) {
					consumer.seekToEnd(tp);
				}
				consumer.position(tp);
			}
			consumer.commitSync();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			consumer.close();
		}
	}

	public void createTopic(TopicView topic) {
		int partition = KafkaConfigs.DEFAULT_KAFKA_PARTITIONS;
		int replication = KafkaConfigs.DEFAULT_KAFKA_REPLICATION_FACTOR;
		Properties topicProp = new Properties();
		topicProp.put("segment.ms", KafkaConfigs.DEFAULT_KAFKA_SEGMENT_MS);
		for (Property prop : topic.getProperties()) {
			if ("replication-factor".equals(prop.getName())) {
				replication = Integer.parseInt(prop.getValue());
			} else if ("partitions".equals(prop.getName())) {
				partition = Integer.parseInt(prop.getValue());
			} else if (KafkaConfigs.VALID_KAFKA_CONFIG_KEYS.contains(prop.getName())) {
				topicProp.setProperty(prop.getName(), prop.getValue());
			}
		}
		Config config = ConfigService.getAppConfig();
		try {
			ZkClient zkClient = null;
			try {
				zkClient = new ZkClient(new ZkConnection(m_zookeeperConnect));
				zkClient.setZkSerializer(new ZKStringSerializer());
				ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(m_zookeeperConnect), false);

				String portalKafkaDeployConfig = config.getProperty(
				      String.format("%s.%s", KafkaConfigs.APOLLO_KAFKA_DEPLOY_PROPERTY_PREFIX, m_idc), null);

				if (!StringUtils.isBlank(portalKafkaDeployConfig)) {
					List<Object> brokerList = new ArrayList<>();
					JSONObject kafkaDepolyConfigs = JSON.parseObject(portalKafkaDeployConfig, JSONObject.class);
					for (String key : kafkaDepolyConfigs.keySet()) {
						if (topic.getName().startsWith(key)) {
							for (Object kafka : kafkaDepolyConfigs.getJSONArray(key)) {
								brokerList.add(kafka);
							}
							break;
						}
					}

					if (brokerList.isEmpty()) {
						JSONArray defaultConfig = kafkaDepolyConfigs.getJSONArray("_default_");
						if (defaultConfig != null) {
							for (Object kafka : defaultConfig) {
								brokerList.add(kafka);
							}
						}
					}

					if (!brokerList.isEmpty()) {
						scala.collection.Map<Object, Seq<Object>> assignments = AdminUtils.assignReplicasToBrokers(
						      JavaConversions.asScalaBuffer(brokerList).toSeq(), partition, replication, -1, -1);
						AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic.getName(), assignments,
						      topicProp, false);
						return;
					}

				}

				AdminUtils.createTopic(zkUtils, topic.getName(), partition, replication, topicProp);

			} finally {
				if (zkClient != null) {
					zkClient.close();
				}
			}
		} catch (Exception e) {
			deleteTopic(topic);
			throw e;
		}

	}

	public void deleteTopic(TopicView topic) {
		ZkClient zkClient = null;
		try {
			zkClient = new ZkClient(new ZkConnection(m_zookeeperConnect));
			ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(m_zookeeperConnect), false);
			zkClient.setZkSerializer(new ZKStringSerializer());

			AdminUtils.deleteTopic(zkUtils, topic.getName());
		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}

	public void configTopic(TopicView topic) {
		ZkClient zkClient = null;
		try {
			zkClient = new ZkClient(new ZkConnection(m_zookeeperConnect));
			zkClient.setZkSerializer(new ZKStringSerializer());
			ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(m_zookeeperConnect), false);
			Properties topicProp = new Properties();
			for (Property prop : topic.getProperties()) {
				if (KafkaConfigs.VALID_KAFKA_CONFIG_KEYS.contains(prop.getName())) {
					topicProp.setProperty(prop.getName(), prop.getValue());
				}
			}

			AdminUtils.changeTopicConfig(zkUtils, topic.getName(), topicProp);

		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
	}

	private KafkaConsumer<String, byte[]> simpleConsumer(String topic, String consumer) {
		Properties config = new Properties();
		config.put("bootstrap.servers", m_bootstrapServers);
		config.put("group.id", consumer);
		config.put("enable.auto.commit", "false");
		config.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
		config.put("key.deserializer", StringDeserializer.class.getCanonicalName());
		return new KafkaConsumer<String, byte[]>(config);
	}

	public boolean isValid() {
		if (StringUtils.isBlank(m_idc) || StringUtils.isBlank(m_bootstrapServers)
		      || StringUtils.isBlank(m_zookeeperConnect)) {
			return false;
		}

		String[] brokers = m_bootstrapServers.split(",");
		for (String broker : brokers) {
			String[] ipport = broker.split(":");
			if (ipport.length != 2) {
				return false;
			}
			if (!KafkaConfigs.IPV4_PATTERN.matcher(ipport[0]).matches()) {
				return false;
			}
		}

		String[] zookeepers = m_zookeeperConnect.split(",");
		for (String zookeeper : zookeepers) {
			String[] ipport = zookeeper.split(":");
			if (ipport.length != 2) {
				return false;
			}
			if (!KafkaConfigs.IPV4_PATTERN.matcher(ipport[0]).matches()) {
				return false;
			}
		}

		return true;
	}
}
