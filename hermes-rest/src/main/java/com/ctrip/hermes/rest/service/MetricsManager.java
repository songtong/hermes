package com.ctrip.hermes.rest.service;

import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;

@Named
public class MetricsManager {

	private static final Logger logger = LoggerFactory.getLogger(MetricsManager.class);

	private MetricRegistry metricRegistry = new MetricRegistry();;

	private Reporter reporter;

	Histogram histogram(String name, String... names) {
		Histogram histogram = metricRegistry.histogram(MetricRegistry.name(name, names));
		return histogram;
	}

	Meter meter(String name, String... names) {
		Meter meter = metricRegistry.meter(MetricRegistry.name(name, names));
		return meter;
	}

	Counter counter(String name, String... names) {
		Counter counter = metricRegistry.counter(MetricRegistry.name(name, names));
		return counter;
	}

	Timer timer(String name, String... names) {
		Timer timer = metricRegistry.timer(MetricRegistry.name(name, names));
		return timer;
	}

	void gauge(final Gauge gauge, String name, String... names) {
		metricRegistry.register(MetricRegistry.name(name, names), new Gauge<Object>() {
			@Override
			public Object getValue() {
				return gauge.getValue();
			}
		});
	}
	
}
