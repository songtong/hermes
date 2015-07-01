package com.ctrip.hermes.kafka.producer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.producer.sender.MessageSender;

@Named(type = MessageSender.class, value = Endpoint.KAFKA)
public class KafkaMessageSender implements MessageSender {

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaMessageSender.class);

	private Map<String, KafkaProducer<String, byte[]>> m_producers = new HashMap<>();;

	@Inject
	private MessageCodec m_codec;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ClientEnvironment m_environment;

	private Properties getProducerProperties(String topic) {
		Properties configs = new Properties();

		try {
			Properties envProperties = m_environment.getProducerConfig(topic);
			configs.putAll(envProperties);
		} catch (IOException e) {
			m_logger.warn("read producer config failed", e);
		}

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

	/**
	 * 
	 * @param msg
	 * @return
	 */
	@Override
	public Future<SendResult> send(ProducerMessage<?> msg) {
		String topic = msg.getTopic();
		String partition = msg.getPartitionKey();

		if (!m_producers.containsKey(topic)) {
			synchronized (m_producers) {
				if (!m_producers.containsKey(topic)) {
					Properties configs = getProducerProperties(topic);
					KafkaProducer<String, byte[]> producer = new KafkaProducer<>(configs);
					m_producers.put(topic, producer);
				}
			}
		}

		KafkaProducer<String, byte[]> producer = m_producers.get(topic);

		byte[] bytes = m_codec.encode(msg);

		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, bytes);

		Future<RecordMetadata> sendResult = null;
		if (msg.getCallback() != null) {
			sendResult = producer.send(record, new KafkaCallback(msg.getCallback()));
		} else {
			sendResult = producer.send(record);
		}

		return new KafkaFuture(sendResult);
	}
}
