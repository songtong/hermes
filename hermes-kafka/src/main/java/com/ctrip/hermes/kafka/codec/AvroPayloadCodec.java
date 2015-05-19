package com.ctrip.hermes.kafka.codec;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.payload.PayloadCodec;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.AVRO)
public class AvroPayloadCodec implements PayloadCodec, Initializable {

	private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

	@Inject
	private ClientEnvironment clientEnv;

	private KafkaAvroSerializer avroSerializer;

	private KafkaAvroDeserializer avroDeserializer;

	@SuppressWarnings("unchecked")
	@Override
	public <T> T decode(byte[] raw, Class<T> clazz) {
		return (T) avroDeserializer.deserialize(null, raw);
	}

	@Override
	public byte[] encode(String topic, Object obj) {
		return avroSerializer.serialize(topic, obj);
	}

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.AVRO;
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, String> configs = new HashMap<>();
		configs.put(SCHEMA_REGISTRY_URL, clientEnv.getGlobalConfig().getProperty(SCHEMA_REGISTRY_URL));

		avroSerializer = new KafkaAvroSerializer();
		avroSerializer.configure(configs, false);

		avroDeserializer = new KafkaAvroDeserializer();
		configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.TRUE.toString());
		avroDeserializer.configure(configs, false);
	}
}
