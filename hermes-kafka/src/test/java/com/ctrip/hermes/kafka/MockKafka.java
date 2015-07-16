package com.ctrip.hermes.kafka;

import java.util.Properties;
import java.util.UUID;

import org.I0Itec.zkclient.ZkClient;

import com.ctrip.hermes.kafka.admin.ZKStringSerializer;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class MockKafka {

	public static final String BROKER_ID = "0";

	public static final String BROKER_HOSTNAME = "localhost";

	public static final String BROKER_PORT = "9092";

	public static String LOCALHOST_BROKER = BROKER_HOSTNAME + ":" + BROKER_PORT;

	public KafkaServerStartable kafkaServer;

	public MockKafka() {
		this(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString(), BROKER_PORT, BROKER_ID);
		start();
	}

	private MockKafka(Properties properties) {
		KafkaConfig kafkaConfig = new KafkaConfig(properties);
		kafkaServer = new KafkaServerStartable(kafkaConfig);
	}

	private MockKafka(String logDir, String port, String brokerId) {
		this(createProperties(logDir, port, brokerId));
		System.out.println("Kafka logdir: " + logDir);
	}

	private static Properties createProperties(String logDir, String port, String brokerId) {
		Properties properties = new Properties();
		properties.put("port", port);
		properties.put("broker.id", brokerId);
		properties.put("log.dirs", logDir);
		properties.put("offsets.topic.replication.factor", "1");
		properties.put("zookeeper.connect", MockZookeeper.ZOOKEEPER_CONNECT);
		return properties;
	}

	public void start() {
		kafkaServer.startup();
		System.out.println("embedded kafka is up");
	}

	public void stop() {
		kafkaServer.shutdown();
		System.out.println("embedded kafka down");
	}

	public void deleteTopic(String topic) {
		ZkClient zkClient = new ZkClient(MockZookeeper.ZOOKEEPER_CONNECT);
		zkClient.setZkSerializer(new ZKStringSerializer());
		if (AdminUtils.topicExists(zkClient, topic)) {
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient);
			System.out.println(topicMetadata);
			AdminUtils.deleteTopic(zkClient, topic);
		}
	}

}