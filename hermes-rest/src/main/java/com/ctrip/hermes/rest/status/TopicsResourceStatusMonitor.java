package com.ctrip.hermes.rest.status;

import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

public enum TopicsResourceStatusMonitor {
	INSTANCE;

	public static final String MSG_PUBLISH_PREFIX = "message-publish";

	private Meter timeoutMeterGlobal = HermesMetricsRegistry.getMetricRegistry().meter(
	      MetricRegistry.name(MSG_PUBLISH_PREFIX, "timeout", "meter"));

	private Meter requestMeterGlobal = HermesMetricsRegistry.getMetricRegistry().meter(
	      MetricRegistry.name(MSG_PUBLISH_PREFIX, "request", "meter"));

	private Histogram requestSizeHistogramGlobal = HermesMetricsRegistry.getMetricRegistry().histogram(
	      MetricRegistry.name(MSG_PUBLISH_PREFIX, "content-length", "histogram"));

	private Timer sendTimerGlobal = HermesMetricsRegistry.getMetricRegistry().timer(
	      MetricRegistry.name(MSG_PUBLISH_PREFIX, "timer"));

	public Timer getSendTimer(String topic) {
		Timer sendTimer = HermesMetricsRegistry.getMetricRegistryByT(topic).timer(
		      MetricRegistry.name(MSG_PUBLISH_PREFIX, "timer"));
		return sendTimer;
	}

	public Timer getSendTimerGlobal() {
		return sendTimerGlobal;
	}

	public void monitorExecutor(final ThreadPoolExecutor executor) {
		HermesMetricsRegistry.getMetricRegistry().register(MetricRegistry.name(MSG_PUBLISH_PREFIX, "active-count"),
		      new Gauge<Integer>() {
			      @Override
			      public Integer getValue() {
				      return executor.getActiveCount();
			      }
		      });
		HermesMetricsRegistry.getMetricRegistry().register(MetricRegistry.name(MSG_PUBLISH_PREFIX, "pool-size"),
		      new Gauge<Integer>() {
			      @Override
			      public Integer getValue() {
				      return executor.getPoolSize();
			      }
		      });
		HermesMetricsRegistry.getMetricRegistry().register(MetricRegistry.name(MSG_PUBLISH_PREFIX, "queue-size"),
		      new Gauge<Integer>() {
			      @Override
			      public Integer getValue() {
				      return executor.getQueue().size();
			      }
		      });
	}

	public void updateRequestMeter(String topic) {
		requestMeterGlobal.mark();

		Meter requestMeter = HermesMetricsRegistry.getMetricRegistryByT(topic).meter(
		      MetricRegistry.name(MSG_PUBLISH_PREFIX, "request", "meter"));
		requestMeter.mark();
	}

	public void updateRequestSizeHistogram(String topic, int length) {
		requestSizeHistogramGlobal.update(length);

		Histogram requestSizeHistogram = HermesMetricsRegistry.getMetricRegistryByT(topic).histogram(
		      MetricRegistry.name(MSG_PUBLISH_PREFIX, "content-length", "histogram"));
		requestSizeHistogram.update(length);
	}

	public void updateTimeoutMeter(String topic) {
		timeoutMeterGlobal.mark();

		Meter timeoutMeter = HermesMetricsRegistry.getMetricRegistryByT(topic).meter(
		      MetricRegistry.name(MSG_PUBLISH_PREFIX, "timeout", "meter"));
		timeoutMeter.mark();
	}
}
