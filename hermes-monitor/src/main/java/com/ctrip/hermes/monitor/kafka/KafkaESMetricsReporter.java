package com.ctrip.hermes.monitor.kafka;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;

public class KafkaESMetricsReporter implements KafkaESMetricsReporterMBean, KafkaMetricsReporter {

	private static final Logger log = LoggerFactory.getLogger(KafkaESMetricsReporter.class);

	private TransportClient client;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private long pollingPeriodInSeconds;

	private boolean enabled;

	private AbstractPollingReporter underlying = null;

	private String esClusterName;

	private String esTransportAddress;

	private TransportClient createESClient() {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", esClusterName).build();
		TransportClient client = new TransportClient(settings);
		String[] transportAddress = esTransportAddress.split(",");
		for (int i = 0; i < transportAddress.length; i++) {
			String[] split = transportAddress[i].split(":");
			client.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
		}
		return client;
	}

	@Override
	public String getMBeanName() {
		return "kafka:type=" + getClass().getName();
	}

	@Override
	public void init(VerifiableProperties props) {
		esClusterName = props.getString("es.cluster.name", "hermes-es");
		esTransportAddress = props.getString("es.transport.address");
		pollingPeriodInSeconds = props.getInt("kafka.metrics.polling.interval.secs", 60);
		enabled = props.getBoolean("kafka.es.reporter.enabled", false);

		if (enabled) {
			log.info("Reporter is enabled and starting...");
			startReporter(pollingPeriodInSeconds);
		} else {
			log.warn("Reporter is disabled");
		}
	}

	@Override
	public void startReporter(long pollingPeriodInSeconds) {
		if (pollingPeriodInSeconds <= 0) {
			throw new IllegalArgumentException("Polling period must be greater than zero");
		}

		synchronized (running) {
			if (running.get()) {
				log.warn("Reporter is already running");
			} else {
				client = createESClient();
				underlying = new ESReporter(Metrics.defaultRegistry(), client);
				underlying.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
				log.info("Started Reporter with es_cluster_name={}, es_transport_address={}, polling_period_secs={}",
				      esClusterName, esTransportAddress, pollingPeriodInSeconds);
				running.set(true);
			}
		}
	}

	@Override
	public void stopReporter() {
		if (!enabled) {
			log.warn("Reporter is disabled");
		} else {
			synchronized (running) {
				if (running.get()) {
					underlying.shutdown();
					client.close();
					running.set(false);
					log.info("Stopped Reporter");
				} else {
					log.warn("Reporter is not running");
				}
			}
		}

	}
}