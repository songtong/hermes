package com.ctrip.hermes.kafka.codec.assist;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

@Named(type = HermesKafkaAvroDeserializer.class)
public class HermesKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer implements Deserializer<Object> {
	@Inject
	private HermesSchemaRestService m_schemaRestService;

	public void setSchemaRestService(HermesSchemaRestService restService) {
		m_schemaRestService = restService;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void configure(Map<String, ?> configs, boolean isKey) {
		Map<String, String> m = new HashMap<String, String>();
		m.put("schema.registry.url", "http://127.0.0.1:8081");
		m.putAll((Map<? extends String, ? extends String>) configs);

		KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(m);
		try {
			int maxSchemaObject = config.getInt(KafkaAvroDeserializerConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG);
			schemaRegistry = new CachedSchemaRegistryClient(m_schemaRestService, maxSchemaObject);
			configureNonClientProperties(config);
		} catch (io.confluent.common.config.ConfigException e) {
			throw new ConfigException(e.getMessage());
		}
	}

	@Override
	public Object deserialize(String s, byte[] bytes) {
		return deserialize(bytes);
	}

	@Override
	public void close() {
	}
}
