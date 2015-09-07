package com.ctrip.hermes.consumer.engine.status;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Gauge;
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

		private static final String MSGS = "messages";

		private static final String CMDS = "commands";

		private final String m_metricsPrefix;

		public ConsumerStatusHolder(Tpg tpg) {

			m_metricsPrefix = getMetricsPrefix(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
		}

		public void pullMessageCmdResultReceived() {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "pullMessageResultReceived")).mark();
		}

		public void pullMessageCmdSent() {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, CMDS, "pullMessageSent"))
			      .mark();
		}

		public void pullMessageCmdResultReadTimeout() {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "pullMessageResultReadTimeout")).mark();
		}

		public void messageReceived(int msgCount) {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "received"))
			      .mark(msgCount);
		}

		public void messageProcessed(int msgCount) {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, MSGS, "processed"))
			      .mark(msgCount);
		}

		public void queryOffsetCmdSent() {
			HermesMetricsRegistry.getMetricRegistry().meter(MetricRegistry.name(m_metricsPrefix, CMDS, "queryOffsetSent"))
			      .mark();
		}

		public void queryOffsetCmdResultReadTimeout() {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "queryOffsetResultReadTimeout")).mark();

		}

		public void queryOffsetCmdResultReceived() {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "queryOffsetResultReceived")).mark();
		}

		public void queryOffsetCmdError() {
			HermesMetricsRegistry.getMetricRegistry()
			      .meter(MetricRegistry.name(m_metricsPrefix, CMDS, "queryOffsetError")).mark();
		}

	}

	private String getMetricsPrefix(String topic, int partition, String group) {
		String metricPrefix = topic + "#" + partition + "@" + group;
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
		return MetricRegistry.name(getMetricsPrefix(topic, partition, group), "localCache", "size");
	}

	public void removeMonitor(String topic, int partition, String group) {
		Tpg tpg = new Tpg(topic, partition, group);
		m_tpg2StatusHolder.remove(tpg);

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

	public void queryOffsetCmdSent(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).queryOffsetCmdSent();
	}

	public void queryOffsetCmdResultReadTimeout(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).queryOffsetCmdResultReadTimeout();
	}

	public void queryOffsetCmdError(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).queryOffsetCmdError();
	}

	public void queryOffsetCmdResultReceived(String topic, int partition, String group) {
		getStatusHolder(topic, partition, group).queryOffsetCmdResultReceived();
	}

	public void messageReceived(String topic, int partition, String group, int msgCount) {
		getStatusHolder(topic, partition, group).messageReceived(msgCount);
	}

	public void messageProcessed(String topic, int partition, String group, int msgCount) {
		getStatusHolder(topic, partition, group).messageProcessed(msgCount);
	}
}
