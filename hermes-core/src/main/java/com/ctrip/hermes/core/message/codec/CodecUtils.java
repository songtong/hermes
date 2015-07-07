package com.ctrip.hermes.core.message.codec;

import java.nio.ByteBuffer;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.transport.netty.Magic;

public class CodecUtils {

	public static ByteBuffer getPayload(ByteBuffer consumerMsg) {
		Magic.readAndCheckMagic(consumerMsg);// skip magic number
		consumerMsg.get();// skip version

		consumerMsg.getInt();// skip whole length
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

	public static void main(String[] args) {
		ProducerMessage<String> proMsg = new ProducerMessage<String>();
		proMsg.setTopic("kafka.SimpleTextTopic");
		proMsg.setBody("Hello Ctrip");
		proMsg.setPartitionKey("MyPartition");
		proMsg.setKey("MyKey");
		proMsg.setBornTime(System.currentTimeMillis());
		DefaultMessageCodec codec = new DefaultMessageCodec();
		byte[] proMsgByte = codec.encode(proMsg);
		ByteBuffer byteBuffer = ByteBuffer.wrap(proMsgByte);
		ByteBuffer payload = getPayload(byteBuffer);
		Object parseObject = JSON.parseObject(payload.array(), String.class);
		System.out.println(parseObject);
	}
}
