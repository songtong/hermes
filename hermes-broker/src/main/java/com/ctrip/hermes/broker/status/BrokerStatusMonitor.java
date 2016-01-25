package com.ctrip.hermes.broker.status;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum BrokerStatusMonitor {
	INSTANCE;

	public void addCacheSizeGauge(final String topic, final int partition, String name, final PageCache<?> pageCache) {
		String pageSizeGaugeName = MetricRegistry.name("cache", name, "pageSize");
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(pageSizeGaugeName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(pageSizeGaugeName, new Gauge<Integer>() {

			@Override
			public Integer getValue() {
				return pageCache.pageSize();
			}
		});
		String pageCountGuageName = MetricRegistry.name("cache", name, "pageCount");
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(pageCountGuageName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(pageCountGuageName, new Gauge<Integer>() {

			@Override
			public Integer getValue() {
				return pageCache.pageCount();
			}
		});
		String hasDataRatio1MinName = MetricRegistry.name("cache", name, "hasDataRatio", "1Min");
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatio1MinName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatio1MinName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "calls"));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "hasData"));
				return Ratio.of(cacheHits.getOneMinuteRate(), cacheCalls.getOneMinuteRate());
			}

		});
		String hasDataRatio5MinName = MetricRegistry.name("cache", name, "hasDataRatio", "5Min");
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatio5MinName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatio5MinName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "calls"));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "hasData"));
				return Ratio.of(cacheHits.getFiveMinuteRate(), cacheCalls.getFiveMinuteRate());
			}

		});
		String hasDataRatio15MinName = MetricRegistry.name("cache", name, "hasDataRatio", "15Min");
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatio15MinName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatio15MinName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "calls"));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "hasData"));
				return Ratio.of(cacheHits.getFifteenMinuteRate(), cacheCalls.getFifteenMinuteRate());
			}

		});
		String hasDataRatioMeanName = MetricRegistry.name("cache", name, "hasDataRatio", "Mean");
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatioMeanName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatioMeanName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "calls"));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name("cache", "hasData"));
				return Ratio.of(cacheHits.getMeanRate(), cacheCalls.getMeanRate());
			}

		});
	}

	public void cacheCall(String topic, int partition, boolean hasData) {
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name("cache", "calls")).mark();
		if (hasData) {
			HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name("cache", "hasData"))
			      .mark();
		}
	}

	public void msgReceived(String topic, int partition, String clientIp, int totalBytes, int msgCount) {
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name("Message", "received"))
		      .mark(msgCount);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition)
		      .meter(MetricRegistry.name("Message", "received", clientIp)).mark(msgCount);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition)
		      .histogram(MetricRegistry.name("Message", "avgBytes")).update(totalBytes / msgCount);
	}

	public void kafkaSend(String topic) {
		HermesMetricsRegistry.getMetricRegistryByT(topic).meter(MetricRegistry.name("KafkaStorage", "send", "meter"))
		      .mark();
	}

	public void msgSaved(String topic, int partition, int count) {
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name("Message", "saved"))
		      .mark(count);
	}

	public void msgDelivered(String topic, int partition, String group, String clientIp, int msgCount) {
		HermesMetricsRegistry.getMetricRegistryByTPG(topic, partition, group)
		      .meter(MetricRegistry.name("Message", "Delivered")).mark(msgCount);
		HermesMetricsRegistry.getMetricRegistryByTPG(topic, partition, group)
		      .meter(MetricRegistry.name("Message", "Delivered", clientIp)).mark(msgCount);
	}
}
