package com.ctrip.hermes.admin.core.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.kafka.KafkaCluster;
import com.ctrip.hermes.admin.core.kafka.KafkaClusterManager;
import com.ctrip.hermes.admin.core.kafka.KafkaClusterOperationResult;
import com.ctrip.hermes.admin.core.service.ConsumerService.ResetOption;

@Named
public class KafkaService {

	@Inject
	private KafkaClusterManager m_kafkaClusterManager;

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaService.class);

	public void resetConsumerOffset(String topic, String consumerGroup, ResetOption resetOption) {
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
				cluster.resetOffset(topic, consumerGroup, resetOption);
				m_logger.info("Reset offset success! Kafka cluster:{}, topic:{}, consumer:{}", cluster.getIdc(), topic,
				      consumerGroup);
			} catch (Exception e) {
				result.addResult(cluster, false, e.getMessage());
				m_logger.warn("Reset offset failed! Kafka cluster:{}, topic:{}, consumer:{}", cluster.getIdc(), topic,
				      consumerGroup, e);
			}
		}

		if (!result.isSuccess()) {
			throw new RuntimeException(result.getMessage());
		}
	}
}