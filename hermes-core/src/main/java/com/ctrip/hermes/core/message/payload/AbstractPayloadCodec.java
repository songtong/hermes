package com.ctrip.hermes.core.message.payload;

public abstract class AbstractPayloadCodec implements PayloadCodec {

	@SuppressWarnings("unchecked")
	@Override
	public <T> T decode(byte[] raw, Class<T> clazz) {
		if (clazz == RawMessage.class) {
			return (T) new RawMessage(raw);
		} else {
			return doDecode(raw, clazz);
		}
	}

	protected abstract <T> T doDecode(byte[] raw, Class<T> clazz);

}
