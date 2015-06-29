package com.ctrip.hermes.kafka.codec;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.payload.AbstractPayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodec;
import com.ctrip.hermes.kafka.codec.assist.HermesKafkaAvroDeserializer;
import com.ctrip.hermes.kafka.codec.assist.HermesKafkaAvroSerializer;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.AVRO)
public class AvroPayloadCodec extends AbstractPayloadCodec implements Initializable {

	static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

	@Inject
	private HermesKafkaAvroSerializer avroSerializer;

	@Inject
	private HermesKafkaAvroDeserializer avroDeserializer;

	@SuppressWarnings("unchecked")
	@Override
	public <T> T doDecode(byte[] raw, Class<T> clazz) {
		return (T) avroDeserializer.deserialize(null, raw);
	}

	@Override
	public byte[] doEncode(String topic, Object obj) {
		return avroSerializer.serialize(topic, obj);
	}

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.AVRO;
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, String> configs = new HashMap<>();

		avroSerializer.configure(configs, false);

		configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.TRUE.toString());
		avroDeserializer.configure(configs, false);
	}
}
