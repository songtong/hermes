package com.ctrip.hermes.monitor.kafka;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class ESMetricsReporter implements MetricsReporter {

	class KafkaMetricItem {

		private String name;

		private String group;

		private String description;

		private Map<String, String> tags;

		private double value;

		private Date time;

		public KafkaMetricItem(KafkaMetric metric) {
			this.name = metric.metricName().name();
			this.group = metric.metricName().group();
			this.description = metric.metricName().description();
			this.tags = metric.metricName().tags();
			this.value = metric.value();
			this.setTime(new Date());
		}

		public String getDescription() {
			return description;
		}

		public String getGroup() {
			return group;
		}

		public String getName() {
			return name;
		}

		public Map<String, String> getTags() {
			return tags;
		}

		public double getValue() {
			return value;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public void setGroup(String group) {
			this.group = group;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setTags(Map<String, String> tags) {
			this.tags = tags;
		}

		public void setValue(double value) {
			this.value = value;
		}

		public Date getTime() {
			return time;
		}

		public void setTime(Date time) {
			this.time = time;
		}
	}

	private static final Logger log = LoggerFactory.getLogger(ESMetricsReporter.class);

	private static final SimpleDateFormat INDEX_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");

	public static final String KAFKA_INDEX_PREFIX = "kafka-";

	static {
		JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
	}

	private TransportClient client;

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
		log.info("Add kafka metric: {} {}", metric.metricName().group(), metric.metricName().name());
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
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", configs.get("es.cluster.name"))
		      .build();
		client = new TransportClient(settings);
		String[] esTransportAddress = String.valueOf(configs.get("es.transport.address")).split(",");
		for (int i = 0; i < esTransportAddress.length; i++) {
			String[] split = esTransportAddress[i].split(":");
			client.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
		}

		scheduler = Executors.newScheduledThreadPool(1);
		int interval = 60;
		long millis = TimeUnit.SECONDS.toMillis(interval);
		scheduler.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				for (Map.Entry<MetricName, KafkaMetric> e : metrics.entrySet()) {
					KafkaMetricItem item = new KafkaMetricItem(e.getValue());
					IndexRequestBuilder builder = client.prepareIndex(
					      KAFKA_INDEX_PREFIX + INDEX_DATE_FORMAT.format(item.getTime()), item.getGroup(), generateId(item));
					String source = JSON.toJSONString(item, SerializerFeature.WriteDateUseDateFormat);
					IndexResponse response = builder.setSource(source).execute().actionGet();
					if (!response.isCreated()) {
						log.warn("Create index failed, {}", response.getId());
					}
				}
			}
		}, millis, millis, TimeUnit.MILLISECONDS);
	}

	private String generateId(KafkaMetricItem item) {
		StringBuilder sb = new StringBuilder();
		sb.append(item.getGroup()).append('_').append(item.getName()).append('_').append(item.getTime().getTime());
		return sb.toString();
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

	@Override
	public void metricRemoval(KafkaMetric metric) {
		metrics.remove(metric.metricName());
	}
}