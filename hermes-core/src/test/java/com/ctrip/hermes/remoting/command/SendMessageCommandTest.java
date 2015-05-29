package com.ctrip.hermes.remoting.command;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.Header;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommandTest extends ComponentTestCase {

	@Test
	public void testEncodePartialDecodecMessage() {
		String topic = "topic1";

		final String strangeString = "{\"1\":{\"str\":\"429bb071-7d14-4da7-9ef1-a6f5b17911b5\"},"
		      + "\"2\":{\"str\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"},\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1},\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"},\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"},\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"},\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";
		Map<String, String> durableProps = new HashMap<>();
		String dkey = UUID.randomUUID().toString();
		String dvalue = UUID.randomUUID().toString();
		durableProps.put(dkey, dvalue);
		durableProps.put("string", strangeString);

		Map<String, String> volatileProps = new HashMap<>();
		volatileProps.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		volatileProps.put("string", strangeString);

		ProducerMessage<String> msg = createProducerMessage(topic, "body", "key", "partition", 100, true, durableProps,
		      volatileProps);

		MessageCodec codec = lookup(MessageCodec.class);

		ByteBuf buf = Unpooled.buffer();
		codec.encode(msg, buf);

		PartialDecodedMessage pdMsg = codec.decodePartial(buf);
		pdMsg.setVolatileProperties(null);

		ByteBuf buf2 = Unpooled.buffer();
		codec.encode(pdMsg, buf2);

		BaseConsumerMessage<?> cmsg = codec.decode(topic, buf2, String.class);

		assertEquals(msg.getBody(), cmsg.getBody());
		assertEquals(msg.getTopic(), cmsg.getTopic());
		assertEquals(msg.getKey(), cmsg.getRefKey());
		assertEquals(dvalue, cmsg.getDurableAppProperty(dkey));

	}

	@Test
	public void testEncodeAndDecodeWithSinglePriorityMessage() {
		String topic = "topic";
		int partition = 100;
		JsonPayloadCodec jsonCodec = new JsonPayloadCodec();
		SendMessageCommand cmd = new SendMessageCommand(topic, 100);

		Map<String, String> appProperties = new HashMap<String, String>();
		appProperties.put("1", "1");
		Map<String, String> sysProperties = new HashMap<String, String>();
		sysProperties.put("2", "2");

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage(topic, "body", "key", "partition", partition, true, appProperties, sysProperties),
		      future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decodedCmd = new SendMessageCommand(topic, partition);
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		Assert.assertEquals(1, decodedCmd.getMessageCount());

		Map<Integer, MessageBatchWithRawData> messageRawDataBatches = decodedCmd.getMessageRawDataBatches();

		Assert.assertEquals(1, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(0));
		Assert.assertFalse(messageRawDataBatches.containsKey(1));

		MessageBatchWithRawData batch = messageRawDataBatches.get(0);

		Assert.assertEquals(1, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));

		List<PartialDecodedMessage> messages = batch.getMessages();
		Assert.assertEquals(1, messages.size());

		PartialDecodedMessage msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		byte[] bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key", msg.getKey());

		Map<String, String> decodedAppProperties = new HermesPrimitiveCodec(msg.getDurableProperties())
		      .readStringStringMap();
		Map<String, String> decodedSysProperties = new HermesPrimitiveCodec(msg.getVolatileProperties())
		      .readStringStringMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals("1", decodedAppProperties.get(PropertiesHolder.APP + "1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals("2", decodedSysProperties.get("2"));

	}

	@Test
	public void testEncodeAndDecodeWithSingleNonPriorityMessage() {
		String topic = "topic";
		int partition = 100;
		JsonPayloadCodec jsonCodec = new JsonPayloadCodec();
		SendMessageCommand cmd = new SendMessageCommand(topic, 100);

		Map<String, String> appProperties = new HashMap<String, String>();
		appProperties.put("1", "1");
		Map<String, String> sysProperties = new HashMap<String, String>();
		sysProperties.put("2", "2");

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage(topic, "body", "key", "partition", partition, false, appProperties, sysProperties),
		      future);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decodedCmd = new SendMessageCommand(topic, partition);
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		Assert.assertEquals(1, decodedCmd.getMessageCount());

		Map<Integer, MessageBatchWithRawData> messageRawDataBatches = decodedCmd.getMessageRawDataBatches();

		Assert.assertEquals(1, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(1));
		Assert.assertFalse(messageRawDataBatches.containsKey(0));

		MessageBatchWithRawData batch = messageRawDataBatches.get(1);

		Assert.assertEquals(1, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));

		List<PartialDecodedMessage> messages = batch.getMessages();
		Assert.assertEquals(1, messages.size());

		PartialDecodedMessage msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		byte[] bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key", msg.getKey());

		Map<String, String> decodedAppProperties = new HermesPrimitiveCodec(msg.getDurableProperties())
		      .readStringStringMap();
		Map<String, String> decodedSysProperties = new HermesPrimitiveCodec(msg.getVolatileProperties())
		      .readStringStringMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals("1", decodedAppProperties.get(PropertiesHolder.APP + "1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals("2", decodedSysProperties.get("2"));

	}

	@Test
	public void testEncodeAndDecodeWithMultipleMessages() {
		String topic = "topic";
		int partition = 100;
		JsonPayloadCodec jsonCodec = new JsonPayloadCodec();
		SendMessageCommand cmd = new SendMessageCommand(topic, 100);

		Map<String, String> appProperties = new HashMap<String, String>();
		appProperties.put("1", "1");
		Map<String, String> sysProperties = new HashMap<String, String>();
		sysProperties.put("2", "2");

		SettableFuture<SendResult> future = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage(topic, "body1", "key1", "partition", partition, false, appProperties, sysProperties),
		      future);

		SettableFuture<SendResult> future1 = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage(topic, "body2", "key2", "partition", partition, false, appProperties, sysProperties),
		      future1);

		SettableFuture<SendResult> future2 = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage(topic, "body3", "key3", "partition", partition, true, appProperties, sysProperties),
		      future2);

		SettableFuture<SendResult> future3 = SettableFuture.create();
		cmd.addMessage(
		      createProducerMessage(topic, "body4", "key4", "partition", partition, true, appProperties, sysProperties),
		      future3);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decodedCmd = new SendMessageCommand(topic, partition);
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		Assert.assertEquals(4, decodedCmd.getMessageCount());

		Map<Integer, MessageBatchWithRawData> messageRawDataBatches = decodedCmd.getMessageRawDataBatches();

		Assert.assertEquals(2, messageRawDataBatches.size());
		Assert.assertTrue(messageRawDataBatches.containsKey(1));
		Assert.assertTrue(messageRawDataBatches.containsKey(0));

		// non-priority
		MessageBatchWithRawData batch = messageRawDataBatches.get(1);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(0));
		Assert.assertTrue(batch.getMsgSeqs().contains(1));

		List<PartialDecodedMessage> messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1
		PartialDecodedMessage msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		byte[] bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body1", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key1", msg.getKey());

		Map<String, String> decodedAppProperties = new HermesPrimitiveCodec(msg.getDurableProperties())
		      .readStringStringMap();
		Map<String, String> decodedSysProperties = new HermesPrimitiveCodec(msg.getVolatileProperties())
		      .readStringStringMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals("1", decodedAppProperties.get(PropertiesHolder.APP + "1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals("2", decodedSysProperties.get("2"));

		// msg2
		msg = messages.get(1);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body2", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key2", msg.getKey());

		decodedAppProperties = new HermesPrimitiveCodec(msg.getDurableProperties()).readStringStringMap();
		decodedSysProperties = new HermesPrimitiveCodec(msg.getVolatileProperties()).readStringStringMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals("1", decodedAppProperties.get(PropertiesHolder.APP + "1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals("2", decodedSysProperties.get("2"));

		// priority
		batch = messageRawDataBatches.get(0);

		Assert.assertEquals(2, batch.getMsgSeqs().size());
		Assert.assertTrue(batch.getMsgSeqs().contains(2));
		Assert.assertTrue(batch.getMsgSeqs().contains(3));

		messages = batch.getMessages();
		Assert.assertEquals(2, messages.size());

		// msg1
		msg = messages.get(0);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body3", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key3", msg.getKey());

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals("1", decodedAppProperties.get(PropertiesHolder.APP + "1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals("2", decodedSysProperties.get("2"));

		// msg2
		msg = messages.get(1);

		Assert.assertTrue(msg.getBornTime() != 0L);

		bodyRawData = new byte[msg.getBody().readableBytes()];
		msg.getBody().readBytes(bodyRawData);
		Assert.assertEquals("body4", jsonCodec.decode(bodyRawData, String.class));

		Assert.assertEquals("key4", msg.getKey());

		decodedAppProperties = new HermesPrimitiveCodec(msg.getDurableProperties()).readStringStringMap();
		decodedSysProperties = new HermesPrimitiveCodec(msg.getVolatileProperties()).readStringStringMap();

		Assert.assertEquals(1, decodedAppProperties.size());
		Assert.assertEquals("1", decodedAppProperties.get(PropertiesHolder.APP + "1"));
		Assert.assertEquals(1, decodedSysProperties.size());
		Assert.assertEquals("2", decodedSysProperties.get("2"));
	}

	public <T> ProducerMessage<T> createProducerMessage(String topic, T body, String key, String partition,
	      int partitionNo, boolean priority, Map<String, String> durableProperties,
	      Map<String, String> volatileProperties) {
		ProducerMessage<T> msg = new ProducerMessage<T>(topic, body);
		msg.setBornTime(System.currentTimeMillis());
		msg.setKey(key);
		msg.setPartitionKey(partition);
		msg.setPartition(partitionNo);
		msg.setPriority(priority);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		for (Map.Entry<String, String> entry : durableProperties.entrySet()) {
			propertiesHolder.addDurableAppProperty(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<String, String> entry : volatileProperties.entrySet()) {
			propertiesHolder.addVolatileProperty(entry.getKey(), entry.getValue());
		}

		msg.setPropertiesHolder(propertiesHolder);

		return msg;
	}
}
