package com.ctrip.hermes.producer.status;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.tuple.Pair;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
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
					      MetricRegistry.name(getMetricsPrefix(topic, partition), "taskQueueSize"), new Gauge<Integer>() {

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
		String metricPrefix = topic + "-" + partition;
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

	public class ProducerStatusHolder {
		private Counter m_messageSubmittedCounter;

		private Meter m_messageSubmittedMeter;

		private Counter m_messageResubmittedCounter;

		private Meter m_messageResubmittedMeter;

		private Counter m_offerFailedCounter;

		private Meter m_offerFailedMeter;

		private Counter m_brokerAcceptedMsgCounter;

		private Meter m_brokerAcceptedMsgMeter;

		private Counter m_brokerAcceptedCmdCounter;

		private Meter m_brokerAcceptedCmdMeter;

		private Histogram m_sendMsgCmdSize;

		private Counter m_wroteToBrokerMsgCounter;

		private Meter m_wroteToBrokerMsgMeter;

		private Counter m_wroteToBrokerCmdCounter;

		private Meter m_wroteToBrokerCmdMeter;

		private Counter m_brokerRejectedMsgCounter;

		private Meter m_brokerRejectedMsgMeter;

		private Counter m_brokerRejectedCmdCounter;

		private Meter m_brokerRejectedCmdMeter;

		private Counter m_sendFailedMsgCounter;

		private Meter m_sendFailedMsgMeter;

		private Counter m_sendFailedCmdCounter;

		private Meter m_sendFailedCmdMeter;

		private Counter m_waitBrokerResultTimeoutMsgCounter;

		private Meter m_waitBrokerResultTimeoutMsgMeter;

		private Counter m_waitBrokerResultTimeoutCmdCounter;

		private Meter m_waitBrokerResultTimeoutCmdMeter;

		private Counter m_brokerResultReceivedMsgCounter;

		private Meter m_brokerResultReceivedMsgMeter;

		private Counter m_brokerResultReceivedCmdCounter;

		private Meter m_brokerResultReceivedCmdMeter;

		private Counter m_waitBrokerAcceptanceTimeoutMsgCounter;

		private Meter m_waitBrokerAcceptanceTimeoutMsgMeter;

		private Counter m_waitBrokerAcceptanceTimeoutCmdCounter;

		private Meter m_waitBrokerAcceptanceTimeoutCmdMeter;

		public ProducerStatusHolder(String topic, int partition) {
			String metricsPrefix = getMetricsPrefix(topic, partition);

			m_messageSubmittedCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "message-submitted-counter"));
			m_messageSubmittedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "message-submitted-meter"));

			m_messageResubmittedCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "message-resubmitted-counter"));
			m_messageResubmittedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "message-resubmitted-meter"));

			m_offerFailedCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "offer-failure-counter"));
			m_offerFailedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "offer-failure-meter"));

			m_brokerAcceptedMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "broker-accepted-msg-counter"));
			m_brokerAcceptedMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "broker-accepted-msg-meter"));
			m_brokerAcceptedCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "broker-accepted-cmd-counter"));
			m_brokerAcceptedCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "sbroker-accepted-cmd-meter"));
			m_sendMsgCmdSize = HermesMetricsRegistry.getMetricRegistry().histogram(
			      MetricRegistry.name(metricsPrefix, "sendMsgCmd-size"));

			m_wroteToBrokerMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "wrote-to-broker-msg-counter"));
			m_wroteToBrokerMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "wrote-to-broker-msg-meter"));
			m_wroteToBrokerCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "wrote-to-broker-cmd-counter"));
			m_wroteToBrokerCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "wrote-to-broker-cmd-meter"));

			m_brokerRejectedMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "broker-rejected-msg-counter"));
			m_brokerRejectedMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "broker-rejected-msg-meter"));
			m_brokerRejectedCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "broker-rejected-cmd-counter"));
			m_brokerRejectedCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "broker-rejected-cmd-meter"));

			m_sendFailedMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "send-fail-msg-counter"));
			m_sendFailedMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "send-fail-msg-meter"));
			m_sendFailedCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "send-fail-cmd-counter"));
			m_sendFailedCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "send-fail-cmd-meter"));

			m_waitBrokerResultTimeoutMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-result-timeout-msg-counter"));
			m_waitBrokerResultTimeoutMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-result-timeout-msg-meter"));
			m_waitBrokerResultTimeoutCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-result-timeout-cmd-counter"));
			m_waitBrokerResultTimeoutCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-result-timeout-cmd-meter"));

			m_brokerResultReceivedMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "broker-result-received-msg-counter"));
			m_brokerResultReceivedMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "broker-result-received-msg-meter"));
			m_brokerResultReceivedCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "broker-result-received-cmd-counter"));
			m_brokerResultReceivedCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "broker-result-received-cmd-meter"));

			m_waitBrokerAcceptanceTimeoutMsgCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-acceptance-timeout-msg-counter"));
			m_waitBrokerAcceptanceTimeoutMsgMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-acceptance-timeout-msg-meter"));
			m_waitBrokerAcceptanceTimeoutCmdCounter = HermesMetricsRegistry.getMetricRegistry().counter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-acceptance-timeout-cmd-counter"));
			m_waitBrokerAcceptanceTimeoutCmdMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "wait-broker-acceptance-timeout-cmd-meter"));
		}

		public void brokerResultReceived(int messageCount) {
			m_brokerResultReceivedMsgCounter.inc(messageCount);
			m_brokerResultReceivedMsgMeter.mark(messageCount);
			m_brokerResultReceivedCmdCounter.inc();
			m_brokerResultReceivedCmdMeter.mark();
		}

		public void waitBrokerAcceptanceTimeout(int messageCount) {
			m_waitBrokerAcceptanceTimeoutMsgCounter.inc(messageCount);
			m_waitBrokerAcceptanceTimeoutMsgMeter.mark(messageCount);
			m_waitBrokerAcceptanceTimeoutCmdCounter.inc();
			m_waitBrokerAcceptanceTimeoutCmdMeter.mark();
		}

		public void messageSubmitted() {
			m_messageSubmittedCounter.inc();
			m_messageSubmittedMeter.mark();
		}

		public void messageResubmitted() {
			m_messageResubmittedCounter.inc();
			m_messageResubmittedMeter.mark();
		}

		public void offerFailed() {
			m_offerFailedCounter.inc();
			m_offerFailedMeter.mark();
		}

		public void brokerAccepted(int messageCount) {
			m_brokerAcceptedMsgCounter.inc(messageCount);
			m_brokerAcceptedMsgMeter.mark(messageCount);
			m_brokerAcceptedCmdCounter.inc();
			m_brokerAcceptedCmdMeter.mark();
			m_sendMsgCmdSize.update(messageCount);
		}

		public void wroteToBroker(int messageCount) {
			m_wroteToBrokerMsgCounter.inc(messageCount);
			m_wroteToBrokerMsgMeter.mark(messageCount);
			m_wroteToBrokerCmdCounter.inc();
			m_wroteToBrokerCmdMeter.mark();
		}

		public void brokerRejected(int messageCount) {
			m_brokerRejectedMsgCounter.inc(messageCount);
			m_brokerRejectedMsgMeter.mark(messageCount);
			m_brokerRejectedCmdCounter.inc();
			m_brokerRejectedCmdMeter.mark();
		}

		public void sendFailed(int messageCount) {
			m_sendFailedMsgCounter.inc(messageCount);
			m_sendFailedMsgMeter.mark(messageCount);
			m_sendFailedCmdCounter.inc();
			m_sendFailedCmdMeter.mark();
		}

		public void waitBrokerResultTimeout(int messageCount) {
			m_waitBrokerResultTimeoutMsgCounter.inc(messageCount);
			m_waitBrokerResultTimeoutMsgMeter.mark(messageCount);
			m_waitBrokerResultTimeoutCmdCounter.inc();
			m_waitBrokerResultTimeoutCmdMeter.mark();
		}

	}

}
