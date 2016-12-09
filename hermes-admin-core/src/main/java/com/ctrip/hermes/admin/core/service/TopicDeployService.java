package com.ctrip.hermes.admin.core.service;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.meta.entity.Property;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

@Named
public class TopicDeployService {

	private static final Logger m_logger = LoggerFactory.getLogger(TopicDeployService.class);

	public static final int DEFAULT_KAFKA_PARTITIONS = 3;

	public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 2;

	// 1 hour
	public static final String DEFAULT_KAFKA_SEGMENT_MS = "3600000";

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
		Map<String, String> zkConnects = m_dsService.getKafkaZookeeperList();
		for (Entry<String, String> zkConnect : zkConnects.entrySet()) {
			ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect.getValue()));
			zkClient.setZkSerializer(new ZKStringSerializer());
			ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect.getValue()), false);
			Properties topicProp = new Properties();
			for (Property prop : topic.getProperties()) {
				if (validKafkaConfigKeys.contains(prop.getName())) {
					topicProp.setProperty(prop.getName(), prop.getValue());
				}
			}

			m_logger.info("config topic in kafka {}, topic {}, prop {}", zkConnect.getKey(), topic.getName(), topicProp);
			AdminUtils.changeTopicConfig(zkUtils, topic.getName(), topicProp);
		}
	}

	/**
	 * @param topic
	 */
	public void createTopicInKafka(TopicView topic) {
		int partition = DEFAULT_KAFKA_PARTITIONS;
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
		Map<String, String> zkConnects = m_dsService.getKafkaZookeeperList();
		try {
			for (Entry<String, String> zkConnect : zkConnects.entrySet()) {
				ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect.getValue()));
				zkClient.setZkSerializer(new ZKStringSerializer());
				ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect.getValue()), false);
				m_logger.info("create topic in kafka {}, topic {}, partition {}, replication {}, prop {}",
				      zkConnect.getKey(), topic.getName(), partition, replication, topicProp);
				AdminUtils.createTopic(zkUtils, topic.getName(), partition, replication, topicProp);

			}
		} catch(Exception e) {
			deleteTopicInKafka(topic);
			throw e;
		}

	}

	/**
	 * @param topic
	 */
	public void deleteTopicInKafka(TopicView topic) {
		Map<String, String> zkConnects = m_dsService.getKafkaZookeeperList();

		for (Entry<String, String> zkConnect : zkConnects.entrySet()) {
			ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect.getValue()));
			ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect.getValue()), false);
			zkClient.setZkSerializer(new ZKStringSerializer());

			m_logger.info("delete topic in kafka {}, topic {}", zkConnect.getKey(), topic.getName());
			AdminUtils.deleteTopic(zkUtils, topic.getName());
		}
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