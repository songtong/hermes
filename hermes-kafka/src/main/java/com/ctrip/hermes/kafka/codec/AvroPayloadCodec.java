package com.ctrip.hermes.kafka.codec;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Map;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.payload.PayloadCodec;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.AVRO)
public class AvroPayloadCodec implements PayloadCodec {

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
	public void configure(Map<String, String> configs) {
		if (avroSerializer == null) {
			avroSerializer = new KafkaAvroSerializer();
			avroSerializer.configure(configs, false);
		}
		if (avroDeserializer == null) {
			avroDeserializer = new KafkaAvroDeserializer();
			configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.TRUE.toString());
			avroDeserializer.configure(configs, false);
		}
	}

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.AVRO;
	}
}
