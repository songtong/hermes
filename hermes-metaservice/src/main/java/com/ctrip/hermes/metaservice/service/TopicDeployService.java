package com.ctrip.hermes.metaservice.service;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.metaservice.view.TopicView;

@Named
public class TopicDeployService {

	private static final Logger m_logger = LoggerFactory.getLogger(TopicDeployService.class);

	public static final int DEFAULT_KAFKA_PARTITIONS = 3;

	public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 2;
	
	// 1 hour
	public static final int DEFAULT_KAFKA_SEGMENT_MS = 3600000;

	private List<String> validKafkaConfigKeys = new ArrayList<String>();

	@Inject
	private StorageService m_dsService;

	public TopicDeployService() {
		validKafkaConfigKeys.add("segment.index.bytes");
		validKafkaConfigKeys.add("segment.jitter.ms");
		validKafkaConfigKeys.add("min.cleanable.dirty.ratio");
		validKafkaConfigKeys.add("retention.bytes");
		validKafkaConfigKeys.add("file.delete.delay.ms");
		validKafkaConfigKeys.add("flush.ms");
		validKafkaConfigKeys.add("cleanup.policy");
		validKafkaConfigKeys.add("unclean.leader.election.enable");
		validKafkaConfigKeys.add("flush.messages");
		validKafkaConfigKeys.add("retention.ms");
		validKafkaConfigKeys.add("min.insync.replicas");
		validKafkaConfigKeys.add("delete.retention.ms");
		validKafkaConfigKeys.add("index.interval.bytes");
		validKafkaConfigKeys.add("segment.bytes");
		validKafkaConfigKeys.add("segment.ms");
	}

	/**
	 * @param topic
	 */
	public void configTopicInKafka(TopicView topic) {
		String zkConnect = m_dsService.getZookeeperList();
		ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect));
		zkClient.setZkSerializer(new ZKStringSerializer());
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		Properties topicProp = new Properties();
		for (Property prop : topic.getProperties()) {
			if (validKafkaConfigKeys.contains(prop.getName())) {
				topicProp.setProperty(prop.getName(), prop.getValue());
			}
		}

		m_logger.info("config topic in kafka, topic {}, prop {}", topic.getName(), topicProp);
		AdminUtils.changeTopicConfig(zkUtils, topic.getName(), topicProp);
	}

	/**
	 * @param topic
	 */
	public void createTopicInKafka(TopicView topic) {
		String zkConnect = m_dsService.getZookeeperList();
		ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect));
		zkClient.setZkSerializer(new ZKStringSerializer());
		int partition = DEFAULT_KAFKA_PARTITIONS;
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		int replication = DEFAULT_KAFKA_REPLICATION_FACTOR;
		Properties topicProp = new Properties();
		topicProp.put("segment.ms", DEFAULT_KAFKA_SEGMENT_MS);
		for (Property prop : topic.getProperties()) {
			if ("replication-factor".equals(prop.getName())) {
				replication = Integer.parseInt(prop.getValue());
			} else if ("partitions".equals(prop.getName())) {
				partition = Integer.parseInt(prop.getValue());
			} else if (validKafkaConfigKeys.contains(prop.getName())) {
				topicProp.setProperty(prop.getName(), prop.getValue());
			}
		}

		m_logger.info("create topic in kafka, topic {}, partition {}, replication {}, prop {}", topic.getName(),
		      partition, replication, topicProp);
		AdminUtils.createTopic(zkUtils, topic.getName(), partition, replication, topicProp);
	}

	/**
	 * @param topic
	 */
	public void deleteTopicInKafka(TopicView topic) {
		String zkConnect = m_dsService.getZookeeperList();

		ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect));
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		zkClient.setZkSerializer(new ZKStringSerializer());

		m_logger.info("delete topic in kafka, topic {}", topic.getName());
		AdminUtils.deleteTopic(zkUtils, topic.getName());
	}
}

class ZKStringSerializer implements ZkSerializer {

	@Override
	public Object deserialize(byte[] bytes) throws ZkMarshallingError {
		if (bytes == null)
			return null;
		else
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
	}

	@Override
	public byte[] serialize(Object data) throws ZkMarshallingError {
		byte[] bytes = null;
		try {
			bytes = data.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError(e);
		}
		return bytes;
	}

}