package com.ctrip.hermes.producer.status;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.tuple.Pair;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum ProducerStatusMonitor {
	INSTANCE;

	private Map<Pair<String, Integer>, ProducerStatusHolder> m_tp2StatusHolder = new ConcurrentHashMap<>();

	private Map<Pair<String, Integer>, Gauge<Integer>> m_tp2TaskQueueGauge = new ConcurrentHashMap<>();

	public void addTaskQueueGauge(String topic, int partition, final BlockingQueue<?> taskQueue) {
		Pair<String, Integer> topicPartition = new Pair<String, Integer>(topic, partition);
		if (!m_tp2TaskQueueGauge.containsKey(topicPartition)) {
			synchronized (this) {
				if (!m_tp2TaskQueueGauge.containsKey(topicPartition)) {
					Gauge<Integer> guage = HermesMetricsRegistry.getMetricRegistry().register(
					      MetricRegistry.name(getMetricsPrefix(topic, partition), "sendBuffer", "size"),
					      new Gauge<Integer>() {

						      @Override
						      public Integer getValue() {
							      return taskQueue.size();
						      }
					      });

					m_tp2TaskQueueGauge.put(topicPartition, guage);
				}
			}
		}
	}

	public Timer getTimer(String topic, int partition, String name) {
		return HermesMetricsRegistry.getMetricRegistry().timer(
		      MetricRegistry.name(getMetricsPrefix(topic, partition), name));
	}

	private String getMetricsPrefix(String topic, int partition) {
		String metricPrefix = topic + "#" + partition;
		return metricPrefix;
	}

	public void messageSubmitted(String topic, int partition) {
		getStatusHolder(topic, partition).messageSubmitted();
	}

	private ProducerStatusHolder getStatusHolder(String topic, int partition) {
		Pair<String, Integer> topicPartition = new Pair<String, Integer>(topic, partition);
		ProducerStatusHolder statusHolder = m_tp2StatusHolder.get(topicPartition);
		if (statusHolder == null) {
			synchronized (this) {
				statusHolder = m_tp2StatusHolder.get(topicPartition);
				if (statusHolder == null) {
					statusHolder = new ProducerStatusHolder(topicPartition.getKey(), topicPartition.getValue());
				}
			}
		}
		return statusHolder;
	}

	public void messageResubmitted(String topic, int partition) {
		getStatusHolder(topic, partition).messageResubmitted();
	}

	public void offerFailed(String topic, int partition) {
		getStatusHolder(topic, partition).offerFailed();
	}

	public void brokerAccepted(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).brokerAccepted(messageCount);
	}

	public void wroteToBroker(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).wroteToBroker(messageCount);
	}

	public void brokerRejected(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).brokerRejected(messageCount);
	}

	public void sendFailed(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).sendFailed(messageCount);
	}

	public void waitBrokerResultTimeout(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).waitBrokerResultTimeout(messageCount);
	}

	public void waitBrokerAcceptanceTimeout(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).waitBrokerAcceptanceTimeout(messageCount);
	}

	public void brokerResultReceived(String topic, int partition, int messageCount) {
		getStatusHolder(topic, partition).brokerResultReceived(messageCount);
	}

	private class ProducerStatusHolder {

		private final String m_metricsPrefix;

		private static final String MSGS = "messages";

		private static final String CMDS = "commands";

		public ProducerStatusHolder(String topic, int partition) {
			m_metricsPrefix = getMetricsPrefix(topic, partition);
		}

		public void brokerResultReceived(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, MSGS, "brokerResultReceived")).mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "brokerResultReceived")).mark();
		}

		public void waitBrokerAcceptanceTimeout(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, MSGS, "waitBrokerAcceptanceTimeout")).mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "waitBrokerAcceptanceTimeout")).mark();
		}

		public void messageSubmitted() {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "submitted"))
			      .mark();
		}

		public void messageResubmitted() {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "resubmitted"))
			      .mark();
		}

		public void offerFailed() {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "offerFailed"))
			      .mark();
		}

		public void brokerAccepted(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "brokerAccepted"))
			      .mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, CMDS, "brokerAccepted"))
			      .mark();
			HermesMetricsRegistry.getMetricRegistry()
			      .histogram(MetricRegistry.name(m_metricsPrefix, MSGS, "brokerAccepted", "histogram"))
			      .update(messageCount);
		}

		public void wroteToBroker(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "wroteToBroker"))
			      .mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, CMDS, "wroteToBroker"))
			      .mark();
		}

		public void brokerRejected(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "brokerRejected"))
			      .mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, CMDS, "brokerRejected"))
			      .mark();
		}

		public void sendFailed(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "sendFailed"))
			      .mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, CMDS, "sendFailed"))
			      .mark();
		}

		public void waitBrokerResultTimeout(int messageCount) {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, MSGS, "waitBrokerResultTimeout")).mark(messageCount);
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "waitBrokerResultTimeout")).mark();
		}

	}

}
