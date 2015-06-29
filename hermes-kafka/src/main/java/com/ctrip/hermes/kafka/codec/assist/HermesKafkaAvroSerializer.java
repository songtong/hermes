package com.ctrip.hermes.kafka.codec.assist;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

@Named(type = HermesKafkaAvroSerializer.class)
public class HermesKafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {
	private boolean isKey;

	@Inject
	private HermesSchemaRestService m_schemaRestService;

	public void setSchemaRestService(HermesSchemaRestService restService) {
		m_schemaRestService = restService;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.isKey = isKey;
		Object maxSchema = configs.get(AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG);
		maxSchema = maxSchema == null ? AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT : maxSchema;
		schemaRegistry = new CachedSchemaRegistryClient(m_schemaRestService, (Integer) maxSchema);
	}

	@Override
	public byte[] serialize(String topic, Object record) {
		String subject;
		if (isKey) {
			subject = topic + "-key";
		} else {
			subject = topic + "-value";
		}
		return serializeImpl(subject, record);
	}

	@Override
	public void close() {
	}
}
