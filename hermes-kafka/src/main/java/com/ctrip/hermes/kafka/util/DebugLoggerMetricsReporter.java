package com.ctrip.hermes.kafka.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.HermesThreadFactory;

public class DebugLoggerMetricsReporter implements MetricsReporter {

	private static final Logger m_logger = LoggerFactory.getLogger(DebugLoggerMetricsReporter.class);

	private Map<MetricName, KafkaMetric> metrics = new HashMap<MetricName, KafkaMetric>();

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
		long millis = TimeUnit.MINUTES.toMillis(1);
		scheduler.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				for (Map.Entry<MetricName, KafkaMetric> e : metrics.entrySet()) {
					m_logger.debug("{} : {}", e.getKey().name(), e.getValue().value());
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

}
