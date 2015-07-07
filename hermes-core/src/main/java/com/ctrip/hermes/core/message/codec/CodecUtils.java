package com.ctrip.hermes.core.message.codec;

import java.nio.ByteBuffer;

import com.ctrip.hermes.core.transport.netty.Magic;

public class CodecUtils {

	/**
	 * 
	 * @param consumerMsg
	 * @return
	 */
	public static ByteBuffer getPayload(ByteBuffer consumerMsg) {
		Magic.readAndCheckMagic(consumerMsg);// skip magic number
		consumerMsg.get();// skip version

		consumerMsg.getInt();// skip total length
		int headerLen = consumerMsg.getInt(); // header length
		consumerMsg.getInt(); // skip body length

		final int CRC_LENGTH = 8;
		consumerMsg.limit(consumerMsg.limit() - CRC_LENGTH);
		consumerMsg.position(consumerMsg.position() + headerLen);
		ByteBuffer result = ByteBuffer.allocate(consumerMsg.remaining());
		result.put(consumerMsg);
		result.rewind();
		return result;
	}

	/**
	 * 
	 * @param consumerMsg
	 * @return
	 */
	public static byte[] getPayload(byte[] consumerMsg) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(consumerMsg);
		return getPayload(byteBuffer).array();
	}

}
