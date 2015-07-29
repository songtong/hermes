package com.ctrip.hermes.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;

public class HermesMetricsRegistry {

	private static MetricRegistry metricRegistry = new MetricRegistry();

	private static HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

	private static JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();;

	static {
		final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
		reporter.start();
	}

	public static void reset() {
		metricRegistry = new MetricRegistry();
		healthCheckRegistry = new HealthCheckRegistry();
		jmxReporter.close();
		jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
		jmxReporter.start();
	}

	public static void close() {
		jmxReporter.close();

		metricRegistry.removeMatching(new MetricFilter() {

			@Override
			public boolean matches(String name, Metric metric) {
				return true;
			}

		});
	}

	public static MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public static HealthCheckRegistry getHealthCheckRegistry() {
		return healthCheckRegistry;
	}
}
