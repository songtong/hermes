package com.ctrip.hermes.core.message.payload;

import java.util.Map;

public interface PayloadCodec {

	public String getType();

	public byte[] encode(String topic, Object obj);

	public <T> T decode(byte[] raw, Class<T> clazz);

	public void configure(Map<String, String> configs);
}
