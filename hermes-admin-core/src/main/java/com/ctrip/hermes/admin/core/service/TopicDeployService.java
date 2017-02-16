package com.ctrip.hermes.admin.core.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.kafka.KafkaCluster;
import com.ctrip.hermes.admin.core.kafka.KafkaClusterManager;
import com.ctrip.hermes.admin.core.kafka.KafkaClusterOperationResult;
import com.ctrip.hermes.admin.core.view.TopicView;

@Named
public class TopicDeployService {

	private static final Logger m_logger = LoggerFactory.getLogger(TopicDeployService.class);

	@Inject
	private KafkaClusterManager m_kafkaClusterManager;

	/**
	 * @param topic
	 */
	public void configTopicInKafka(TopicView topic) {
		Map<String, KafkaCluster> clusters;
		try {
			clusters = m_kafkaClusterManager.listClusters();
		} catch (Exception e) {
			m_logger.warn("Failed to list clusters!", e);
			throw new RuntimeException("Failed to list clusters!");
		}

		KafkaClusterOperationResult result = new KafkaClusterOperationResult();
		for (KafkaCluster cluster : clusters.values()) {
			try {
				cluster.configTopic(topic);
				m_logger.info("Config topic success! Kafka cluster:{}, topic:{}, props:{}.", cluster.getIdc(),
				      topic.getName(), topic.getProperties());
			} catch (Exception e) {
				result.addResult(cluster, false, e.getMessage());
				m_logger.warn("Config topic failed! Kafka cluster:{}, topic:{}, props:{}.", cluster.getIdc(),
				      topic.getName(), topic.getProperties(), e);
			}
		}
		if (!result.isSuccess()) {
			throw new RuntimeException(result.getMessage());
		}
	}

	/**
	 * @param topic
	 */
	public void createTopicInKafka(TopicView topic) {
		Map<String, KafkaCluster> clusters;
		try {
			clusters = m_kafkaClusterManager.listClusters();
		} catch (Exception e) {
			m_logger.warn("Failed to list clusters!", e);
			throw new RuntimeException("Failed to list clusters!");
		}

		KafkaClusterOperationResult result = new KafkaClusterOperationResult();
		for (KafkaCluster cluster : clusters.values()) {
			try {
				cluster.createTopic(topic);
				m_logger.info("Create topic success! Kafka cluster:{}, topic:{}.", cluster.getIdc(), topic.getName());
			} catch (Exception e) {
				result.addResult(cluster, false, e.getMessage());
				m_logger.warn("Create topic failed! Kafka cluster:{}, topic:{}.", cluster.getIdc(), topic.getName(), e);
			}
		}
		if (!result.isSuccess()) {
			throw new RuntimeException(result.getMessage());
		}
	}

	/**
	 * @param topic
	 */
	public void deleteTopicInKafka(TopicView topic) {
		Map<String, KafkaCluster> clusters;
		try {
			clusters = m_kafkaClusterManager.listClusters();
		} catch (Exception e) {
			m_logger.warn("Failed to list clusters!", e);
			throw new RuntimeException("Failed to list clusters!");
		}

		KafkaClusterOperationResult result = new KafkaClusterOperationResult();
		for (KafkaCluster cluster : clusters.values()) {
			try {
				cluster.deleteTopic(topic);
				m_logger.info("Delete topic success! Kafka cluster:{}, topic:{}.", cluster.getIdc(), topic.getName());
			} catch (Exception e) {
				result.addResult(cluster, false, e.getMessage());
				m_logger.warn("Delete topic failed! Kafka cluster:{}, topic:{}.", cluster.getIdc(), topic.getName(), e);
			}
		}
		if (!result.isSuccess()) {
			throw new RuntimeException(result.getMessage());
		}
	}
}
