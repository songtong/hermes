package com.ctrip.hermes.core.message.payload;

/**
 * Suppress encode/decode when input is RawMessage
 * @author marsqing
 *
 */
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

	@Override
	public byte[] encode(String topic, Object obj) {
		if (obj instanceof RawMessage) {
			return ((RawMessage) obj).getEncodedMessage();
		} else {
			return doEncode(topic, obj);
		}
	}

	protected abstract byte[] doEncode(String topic, Object obj);

	protected abstract <T> T doDecode(byte[] raw, Class<T> clazz);

}
