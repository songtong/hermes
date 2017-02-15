package com.ctrip.hermes.admin.core.kafka;

public class KafkaClusterOperationResult {
	private boolean m_success = true;

	private StringBuilder m_message = new StringBuilder();

	public void addResult(KafkaCluster cluster, boolean success, String message) {
		if (!success) {
			m_success = false;
			m_message.append("Kafka cluster ");
			m_message.append(cluster.getIdc());
			m_message.append(":");
			m_message.append(message);
			m_message.append(".");
		}
	}

	public String getMessage() {
		return m_message.toString();
	}

	public boolean isSuccess() {
		return m_success;
	}
}
