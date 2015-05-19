package com.ctrip.hermes.core.message.payload;

import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.JSON)
public class JsonPayloadCodec implements PayloadCodec {

	@Override
	public <T> T decode(byte[] bytes, Class<T> clazz) {
		return JSON.parseObject(bytes, clazz);
	}

	@Override
	public byte[] encode(String topic, Object input) {
		return JSON.toJSONBytes(input);
	}

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.JSON;
	}

}
