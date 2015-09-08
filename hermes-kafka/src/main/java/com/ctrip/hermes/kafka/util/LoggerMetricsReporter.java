package com.ctrip.hermes.kafka.util;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class LoggerMetricsReporter implements MetricsReporter {

	private static final Logger m_logger = LoggerFactory.getLogger(LoggerMetricsReporter.class);

	private Map<MetricName, KafkaMetric> metrics = new TreeMap<MetricName, KafkaMetric>(new Comparator<MetricName>() {

		@Override
		public int compare(MetricName o1, MetricName o2) {
			if (!(o1.group().equals(o2.group()))) {
				return o1.group().compareTo(o2.group());
			}
			if (!(o1.name().equals(o2.name()))) {
				return o1.name().compareTo(o2.name());
			}
			if (!(o1.tags().toString().equals(o2.tags().toString()))) {
				return o1.tags().toString().compareTo(o2.tags().toString());
			}
			return 0;
		}

	});

	private ScheduledExecutorService scheduler;

	private void addMetric(KafkaMetric metric) {
		metrics.put(metric.metricName(), metric);
	}

	@Override
	public void close() {
		metrics.clear();
		if (scheduler != null)
			scheduler.shutdown();
	}

	@Override
	public void configure(Map<String, ?> configs) {
		scheduler = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("KafkaMetricsLogger", true));
		ClientEnvironment env = PlexusComponentLocator.lookup(ClientEnvironment.class);
		int interval = 60;
		try {
			Properties producerConfig = env.getProducerConfig("");
			interval = Integer.parseInt(producerConfig.getProperty("metric.reporters.interval.second"));
		} catch (IOException e) {
			m_logger.warn(e.getMessage());
		}
		long millis = TimeUnit.SECONDS.toMillis(interval);
		scheduler.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				for (Map.Entry<MetricName, KafkaMetric> e : metrics.entrySet()) {
					m_logger.info("{} : {}", getMetricKey(e.getKey()), e.getValue().value());
				}
			}
		}, millis, millis, TimeUnit.MILLISECONDS);
	}

	@Override
	public void init(List<KafkaMetric> metrics) {
		for (KafkaMetric metric : metrics) {
			addMetric(metric);
		}
	}

	@Override
	public void metricChange(KafkaMetric metric) {
		addMetric(metric);
	}

	private String getMetricKey(MetricName metricName) {
		StringBuilder sb = new StringBuilder();
		sb.append(metricName.group()).append('|').append(metricName.name()).append('|').append(metricName.tags());
		return sb.toString();
	}
}
