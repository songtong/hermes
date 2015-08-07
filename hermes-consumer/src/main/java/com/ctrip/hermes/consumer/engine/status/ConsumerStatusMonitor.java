package com.ctrip.hermes.consumer.engine.status;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum ConsumerStatusMonitor {
	INSTANCE;

	private Map<Tpg, ConsumerStatusHolder> m_tpg2StatusHolder = new ConcurrentHashMap<>();

	private Map<Tpg, Gauge<Integer>> m_tpg2TaskQueueGauge = new ConcurrentHashMap<>();

	private class ConsumerStatusHolder {

		private Meter m_pullMsgCmdResultReceivedMeter;

		private Meter m_pullMsgCmdSentMeter;

		private Meter m_pullMsgCmdResultReadTimeoutMeter;

		private Meter m_messageReceivedMeter;

		private Meter m_messageProcessedMeter;

		public ConsumerStatusHolder(Tpg tpg) {

			String metricsPrefix = getMetricsPrefix(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());

			m_pullMsgCmdResultReceivedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "pull-msg-cmd-result-received-meter"));

			m_pullMsgCmdSentMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "pull-msg-cmd-sent-meter"));

			m_pullMsgCmdResultReadTimeoutMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "pull-msg-cmd-result-read-timeout-meter"));

			m_messageReceivedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "msg-received-meter"));

			m_messageProcessedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(metricsPrefix, "msg-processed-meter"));
		}

		public void pullMessageCmdResultReceived() {
			m_pullMsgCmdResultReceivedMeter.mark();
		}

		public void pullMessageCmdSent() {
			m_pullMsgCmdSentMeter.mark();
		}

		public void pullMessageCmdResultReadTimeout() {
			m_pullMsgCmdResultReadTimeoutMeter.mark();
		}

		public void messageReceived(int msgCount) {
			m_messageReceivedMeter.mark(msgCount);
		}

		public void messageProcessed(int msgCount) {
			m_messageProcessedMeter.mark(msgCount);
		}

	}

	private String getMetricsPrefix(String topic, int partition, String group) {
		String metricPrefix = topic + "-" + partition + "-" + group;
		return metricPrefix;
	}

	private ConsumerStatusHolder getStatusHolder(String topic, int partition, String group) {
		Tpg tpg = new Tpg(topic, partition, group);
		ConsumerStatusHolder statusHolder = m_tpg2StatusHolder.get(tpg);
		if (statusHolder == null) {
			synchronized (this) {
				statusHolder = m_tpg2StatusHolder.get(tpg);
				if (statusHolder == null) {
					statusHolder = new ConsumerStatusHolder(tpg);
				}
			}
		}
		return statusHolder;
	}

	public void addMessageQueueGuage(String topic, int partition, String group, final BlockingQueue<?> queue) {
		Tpg tpg = new Tpg(topic, partition, group);
		if (!m_tpg2TaskQueueGauge.containsKey(tpg)) {
			synchronized (this) {
				if (!m_tpg2TaskQueueGauge.containsKey(tpg)) {
					Gauge<Integer> guage = HermesMetricsRegistry.getMetricRegistry().register(
					      getMessageQueueGuageName(topic, partition, group), new Gauge<Integer>() {

						      @Override
						      public Integer getValue() {
							      return queue.size();
						      }
					      });

					m_tpg2TaskQueueGauge.put(tpg, guage);
				}
			}
		}
	}

	private String getMessageQueueGuageName(String topic, int partition, String group) {
		return MetricRegistry.name(getMetricsPrefix(topic, partition, group), "messageQueue");
	}

	public void removeMonitor(String topic, int partition, String group) {
		final String metricsPrefix = getMetricsPrefix(topic, partition, group);

		HermesMetricsRegistry.getMetricRegistry().removeMatching(new MetricFilter() {

			@Override
			public boolean matches(String name, Metric metric) {
				return name.startsWith(metricsPrefix);
			}
		});
	}

	public Timer getTimer(String topic, int partition, String group, String name) {
		return HermesMetricsRegistry.getMetricRegistry().timer(
		      MetricRegistry.name(getMetricsPrefix(topic, partition, group), name));
	}

	public void pullMessageCmdResultReceived(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).pullMessageCmdResultReceived();
	}

	public void pullMessageCmdSent(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).pullMessageCmdSent();
	}

	public void pullMessageCmdResultReadTimeout(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).pullMessageCmdResultReadTimeout();
	}

	public void messageReceived(String topic, int partition, String group, int msgCount) {
		getStatusHolder(topic, partition, group).messageReceived(msgCount);
	}

	public void messageProcessed(String topic, int partition, String group, int msgCount) {
		getStatusHolder(topic, partition, group).messageProcessed(msgCount);
	}
}
