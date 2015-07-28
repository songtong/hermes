package com.ctrip.hermes.rest.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;

public class RestMetricsRegistry {

	private static MetricRegistry metricRegistry = new MetricRegistry();

	private static HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

	static{
		final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
		reporter.start();
	}
	
	public static void reset(){
		metricRegistry = new MetricRegistry();
		healthCheckRegistry = new HealthCheckRegistry();
		final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
		reporter.start();
	}
	
	public static MetricRegistry getMetricRegistry(){
		return metricRegistry;
	}
	
	public static HealthCheckRegistry getHealthCheckRegistry(){
		return healthCheckRegistry;
	}
}
