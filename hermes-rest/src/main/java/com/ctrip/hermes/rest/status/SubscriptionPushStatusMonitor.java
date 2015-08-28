package com.ctrip.hermes.rest.status;

import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

public enum SubscriptionPushStatusMonitor {

	INSTANCE;

	public static final String MSG_SUBSCRIPTION_PREFIX = "message-subscription";

	private Meter failedMeterGlobal = HermesMetricsRegistry.getMetricRegistry().meter(
	      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, "failed", "meter"));

	private Meter requestMeterGlobal = HermesMetricsRegistry.getMetricRegistry().meter(
	      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, "request", "meter"));

	private Histogram requestSizeHistogramGlobal = HermesMetricsRegistry.getMetricRegistry().histogram(
	      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, "body-size", "histogram"));

	private Timer pushTimerGlobal = HermesMetricsRegistry.getMetricRegistry().timer(
	      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, "timer"));

	public Timer getPushTimer(Tge tpge) {
		Timer pushTimer = HermesMetricsRegistry.getMetricRegistry().timer(
		      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, tpge.toString(), "timer"));
		return pushTimer;
	}

	public Timer getPushTimerGlobal() {
		return pushTimerGlobal;
	}

	public void monitorConsumerHolderSize(final Map<SubscriptionView, ConsumerHolder> consumerHolders) {
		HermesMetricsRegistry.getMetricRegistry().register(MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, "holders"),
		      new Gauge<Integer>() {
			      @Override
			      public Integer getValue() {
				      return consumerHolders.size();
			      }
		      });
	}

	public void removeMonitor(final String topic, final String group) {
		HermesMetricsRegistry.getMetricRegistry().removeMatching(new MetricFilter() {

			@Override
			public boolean matches(String name, Metric metric) {
				if (name.startsWith(MSG_SUBSCRIPTION_PREFIX)) {
					if (name.contains(topic) && name.contains(group)) {
						return true;
					}
				}
				return false;
			}

		});
	}

	public void updateFailedMeter(Tge tpge) {
		failedMeterGlobal.mark();

		Meter failedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
		      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, tpge.toString(), "failed", "meter"));
		failedMeter.mark();
	}

	public void updateRequestMeter(Tge tpge) {
		requestMeterGlobal.mark();

		Meter requestMeter = HermesMetricsRegistry.getMetricRegistry().meter(
		      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, tpge.toString(), "request", "meter"));
		requestMeter.mark();
	}

	public void updateRequestSizeHistogram(Tge tpge, int length) {
		requestSizeHistogramGlobal.update(length);

		Histogram requestSizeHistogram = HermesMetricsRegistry.getMetricRegistry().histogram(
		      MetricRegistry.name(MSG_SUBSCRIPTION_PREFIX, tpge.toString(), "body-size", "histogram"));
		requestSizeHistogram.update(length);
	}
}
