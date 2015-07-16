package com.ctrip.hermes.core.transport.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.HermesCoreBaseTest;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.meta.entity.Codec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class PullMessageResultCommandTest extends HermesCoreBaseTest {

	@Test
	public void test() throws Exception {
		String topic = "topic";
		long bornTime = System.currentTimeMillis();

		PullMessageResultCommand cmd = new PullMessageResultCommand();
		List<TppConsumerMessageBatch> batches = new ArrayList<TppConsumerMessageBatch>();

		final Pair<MessageMeta, PartialDecodedMessage> priorityMsg1 = createMsgAndMetas(1, topic, "key1", "body1",
		      Arrays.asList(new Pair<String, String>("a", "A")), bornTime, 0, -1, 0, false);
		final Pair<MessageMeta, PartialDecodedMessage> priorityMsg2 = createMsgAndMetas(2, topic, "key2", "body2",
		      Collections.<Pair<String, String>> emptyList(), bornTime, 0, -1, 0, false);

		final Pair<MessageMeta, PartialDecodedMessage> nonPriorityMsg1 = createMsgAndMetas(1, topic, "key3", "body3",
		      Arrays.asList(new Pair<String, String>("b", "B"), new Pair<String, String>("d", "D")), bornTime, 0, -1, 1,
		      false);
		final Pair<MessageMeta, PartialDecodedMessage> nonPriorityMsg2 = createMsgAndMetas(2, topic, "key4", "body4",
		      null, bornTime, 0, -1, 1, false);

		final Pair<MessageMeta, PartialDecodedMessage> resendMsg1 = createMsgAndMetas(1, topic, "key5", "body5",
		      Arrays.asList(new Pair<String, String>("b", "B"), new Pair<String, String>("d", "D")), bornTime, 1, 100, 0,
		      true);
		final Pair<MessageMeta, PartialDecodedMessage> resendMsg2 = createMsgAndMetas(2, topic, "key6", "body6",
		      Arrays.asList(new Pair<String, String>("f", "F")), bornTime, 0, 101, 1, true);

		addBatch(batches, topic, 1, Arrays.asList(priorityMsg1, priorityMsg2), false, 0);
		addBatch(batches, topic, 1, Arrays.asList(nonPriorityMsg1, nonPriorityMsg2), false, 1);
		addBatch(batches, topic, 1, Arrays.asList(resendMsg1), true, 0);
		addBatch(batches, topic, 1, Arrays.asList(resendMsg2), true, 1);
		cmd.addBatches(batches);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		PullMessageResultCommand decodedCmd = new PullMessageResultCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		List<TppConsumerMessageBatch> decodedBatches = decodedCmd.getBatches();

		assertEquals(4, decodedBatches.size());

		assertBatch(decodedBatches.get(0), topic, 1, 0, Arrays.asList(priorityMsg1.getKey(), priorityMsg2.getKey()),
		      Arrays.asList(priorityMsg1.getValue(), priorityMsg2.getValue()));
		assertBatch(decodedBatches.get(1), topic, 1, 1,
		      Arrays.asList(nonPriorityMsg1.getKey(), nonPriorityMsg2.getKey()),
		      Arrays.asList(nonPriorityMsg1.getValue(), nonPriorityMsg2.getValue()));
		assertBatch(decodedBatches.get(2), topic, 1, 0, Arrays.asList(resendMsg1.getKey()),
		      Arrays.asList(resendMsg1.getValue()));
		assertBatch(decodedBatches.get(3), topic, 1, 1, Arrays.asList(resendMsg2.getKey()),
				Arrays.asList(resendMsg2.getValue()));

	}

	@SuppressWarnings("rawtypes")
	private void assertBatch(TppConsumerMessageBatch actual, String topic, int partition, int priority,
	      List<MessageMeta> expectedMetas, List<PartialDecodedMessage> expectedMsgs) {
		assertEquals(topic, actual.getTopic());
		assertEquals(partition, actual.getPartition());
		assertEquals(priority, actual.getPriority());

		List<MessageMeta> messageMetas = actual.getMessageMetas();
		assertEquals(expectedMetas.size(), messageMetas.size());
		for (int i = 0; i < expectedMetas.size(); i++) {
			MessageMeta expectedMeta = expectedMetas.get(i);
			MessageMeta actualMeta = messageMetas.get(i);
			assertEquals(expectedMeta.getId(), actualMeta.getId());
			assertEquals(expectedMeta.getRemainingRetries(), actualMeta.getRemainingRetries());
			assertEquals(expectedMeta.getOriginId(), actualMeta.getOriginId());
			assertEquals(expectedMeta.getPriority(), actualMeta.getPriority());
			assertEquals(expectedMeta.isResend(), actualMeta.isResend());
		}

		List<ConsumerMessage<String>> actualMsgs = decodeBatch(actual, partition, messageMetas);

		assertEquals(expectedMsgs.size(), actualMsgs.size());
		for (int i = 0; i < expectedMsgs.size(); i++) {
			ConsumerMessage<String> actualMsg = actualMsgs.get(i);
			PartialDecodedMessage expectedMsg = expectedMsgs.get(i);
			assertEquals(topic, ((BaseConsumerMessageAware) actualMsg).getBaseConsumerMessage().getTopic());

			assertEquals(expectedMsg.getKey(), ((BaseConsumerMessageAware) actualMsg).getBaseConsumerMessage().getRefKey());

			assertEquals(new JsonPayloadCodec().decode(expectedMsg.readBody(), String.class), actualMsg.getBody());

			Map<String, String> expectedProperites = readProperties(expectedMsg.getDurableProperties());

			if (expectedProperites != null) {
				assertEquals(expectedProperites, ((BaseConsumerMessageAware) actualMsg).getBaseConsumerMessage()
				      .getPropertiesHolder().getDurableProperties());
			} else {
				assertTrue(((BaseConsumerMessageAware) actualMsg).getBaseConsumerMessage().getPropertiesHolder()
				      .getDurableProperties().isEmpty());
			}

			assertEquals(expectedMsg.getRemainingRetries(), ((BaseConsumerMessageAware) actualMsg)
			      .getBaseConsumerMessage().getRemainingRetries());
			assertEquals(expectedMsg.getBornTime(), actualMsg.getBornTime());
		}
	}

	@SuppressWarnings("unchecked")
	private List<ConsumerMessage<String>> decodeBatch(TppConsumerMessageBatch batch, int partition,
	      List<MessageMeta> messageMetas) {
		MessageCodec messageCodec = lookup(MessageCodec.class);

		List<ConsumerMessage<String>> msgs = new ArrayList<ConsumerMessage<String>>();

		for (int j = 0; j < messageMetas.size(); j++) {
			BaseConsumerMessage<String> baseMsg = (BaseConsumerMessage<String>) messageCodec.decode(batch.getTopic(),
			      batch.getData(), String.class);
			BrokerConsumerMessage<String> brokerMsg = new BrokerConsumerMessage<String>(baseMsg);
			MessageMeta messageMeta = messageMetas.get(j);
			brokerMsg.setPartition(partition);
			brokerMsg.setPriority(messageMeta.getPriority() == 0 ? true : false);
			brokerMsg.setResend(messageMeta.isResend());
			brokerMsg.setMsgSeq(messageMeta.getId());

			msgs.add(brokerMsg);
		}

		return msgs;
	}

	private void addBatch(List<TppConsumerMessageBatch> batches, String topic, int partition,
	      final List<Pair<MessageMeta, PartialDecodedMessage>> msgs, boolean isResend, int priority) {
		TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
		List<MessageMeta> metas = new ArrayList<MessageMeta>();
		for (Pair<MessageMeta, PartialDecodedMessage> msg : msgs) {
			metas.add(msg.getKey());
		}
		batch.addMessageMetas(metas);

		batch.setTopic(topic);
		batch.setPartition(partition);
		batch.setResend(isResend);
		batch.setPriority(priority);

		batch.setTransferCallback(new TransferCallback() {

			@Override
			public void transfer(ByteBuf out) {
				for (Pair<MessageMeta, PartialDecodedMessage> msg : msgs) {
					lookup(MessageCodec.class).encodePartial(msg.getValue(), out);
				}
			}

		});
		batches.add(batch);
	}

	private Pair<MessageMeta, PartialDecodedMessage> createMsgAndMetas(long id, String topic, String key, String body,
	      List<Pair<String, String>> durableProperties, long bornTime, int remainingRetries, long originId,
	      int priority, boolean isResend) {
		Pair<MessageMeta, PartialDecodedMessage> pair = new Pair<MessageMeta, PartialDecodedMessage>();
		pair.setKey(createMessageMeta(id, remainingRetries, originId, priority, isResend));
		pair.setValue(createPartialDecodedMessage(topic, key, body, durableProperties, remainingRetries, bornTime));
		return pair;
	}

	private MessageMeta createMessageMeta(long id, int remainingRetries, long originId, int priority, boolean isResend) {
		MessageMeta meta = new MessageMeta(id, remainingRetries, originId, priority, isResend);
		return meta;
	}

	private PartialDecodedMessage createPartialDecodedMessage(String topic, String key, String body,
	      List<Pair<String, String>> durableProperties, int remainingRetries, long bornTime) {
		PartialDecodedMessage partialMsg = new PartialDecodedMessage();

		partialMsg.setRemainingRetries(remainingRetries);
		ByteBuf buf = Unpooled.buffer();
		writeProperties(durableProperties, buf);
		partialMsg.setDurableProperties(buf);
		partialMsg.setBody(Unpooled.wrappedBuffer(new JsonPayloadCodec().encode(topic, body)));
		partialMsg.setBornTime(bornTime);
		partialMsg.setKey(key);
		partialMsg.setBodyCodecType(Codec.JSON);

		return partialMsg;
	}
}
