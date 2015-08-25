package com.ctrip.hermes.broker.status;

import com.codahale.metrics.MetricRegistry;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum BrokerStatusMonitor {
	INSTANCE;

	public class BrokerStatusHolder {
	}

	public void kafkaSend(String topic) {
		HermesMetricsRegistry.getMetricRegistryByT(topic).meter(MetricRegistry.name("KafkaStorage", "send", "meter"))
		      .mark();
	}
}
