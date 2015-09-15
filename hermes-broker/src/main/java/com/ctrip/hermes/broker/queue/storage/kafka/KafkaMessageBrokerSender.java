package com.ctrip.hermes.broker.queue.storage.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

public class KafkaMessageBrokerSender {

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaMessageBrokerSender.class);

	private KafkaProducer<String, byte[]> m_producer;

	private MetaService m_metaService;

	public KafkaMessageBrokerSender(String topic, MetaService metaService) {
		this.m_metaService = metaService;
		Properties configs = getProducerProperties(topic);
		m_producer = new KafkaProducer<>(configs);
		m_logger.debug("Kafka broker sender for {} initialized.", topic);
	}

	private Properties getProducerProperties(String topic) {
		Properties configs = new Properties();

		List<Partition> partitions = m_metaService.listPartitionsByTopic(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String producerDatasource = partitions.get(0).getWriteDatasource();
		Storage producerStorage = m_metaService.findStorageByTopic(topic);
		if (producerStorage == null) {
			return configs;
		}

		for (Datasource datasource : producerStorage.getDatasources()) {
			if (producerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					configs.put(prop.getValue().getName(), prop.getValue().getValue());
				}
				break;
			}
		}

		return overrideByCtripDefaultSetting(configs);
	}

	/**
	 * 
	 * @param producerProp
	 * @return
	 */
	private Properties overrideByCtripDefaultSetting(Properties producerProp) {
		producerProp.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		producerProp.put("key.serializer", StringSerializer.class.getCanonicalName());

		if (!producerProp.containsKey("client.id")) {
			producerProp.put("client.id", Networks.forIp().getLocalHostAddress());
		}
		if (!producerProp.containsKey("block.on.buffer.full")) {
			producerProp.put("block.on.buffer.full", false);
		}
		if (!producerProp.containsKey("linger.ms")) {
			producerProp.put("linger.ms", 50);
		}
		if (!producerProp.containsKey("retries")) {
			producerProp.put("retries", 3);
		}

		return producerProp;
	}

	public void send(String topic, String partitionKey, byte[] array) {
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partitionKey, array);
		m_producer.send(record);
	}

}
