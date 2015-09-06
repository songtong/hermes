package com.ctrip.hermes.broker.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.unidal.dal.jdbc.test.JdbcTestHelper;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.meta.entity.Storage;
import com.google.common.util.concurrent.SettableFuture;

public class ConsumeTest extends BaseBrokerTest {

	@Override
	protected void doBefore() throws Exception {
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testPullMessage() throws Exception {
		String topic = "order_new";
		int partition = 0;
		String groupId = "group1";
		int size = 10;
		long expireTime = Long.MAX_VALUE;

		final AtomicReference<PullMessageResultCommand> pullMsgResultCmdRef = new AtomicReference<>();

		final CountDownLatch latch = new CountDownLatch(1);
		setCommandHandler(new CommandHandler() {

			@Override
			public void handle(Command arg) {
				if (arg instanceof PullMessageResultCommand) {
					pullMsgResultCmdRef.set((PullMessageResultCommand) arg);
					latch.countDown();
				}
			}
		});

		PullMessageCommand pullMsgcmd = new PullMessageCommand(topic, partition, groupId, size, expireTime);
		PullMessageCommand decodedPullMsgCmd = serializeAndDeserialize(pullMsgcmd);

		CommandProcessorContext ctx = new CommandProcessorContext(decodedPullMsgCmd, m_channel);
		CommandProcessorManager cmdProcessorMgr = lookup(CommandProcessorManager.class);
		cmdProcessorMgr.offer(ctx);
		Thread.sleep(1000);

		List<ProducerMessage<String>> pmsgs = new ArrayList<>();
		pmsgs.add(createProducerMessage(topic, uuid(), uuid(), partition, System.currentTimeMillis(), false));
		Tpp tpp = new Tpp(topic, partition, false);

		SendMessageCommand sendMsgCmd = new SendMessageCommand(topic, 0);
		for (ProducerMessage<String> pmsg : pmsgs) {
			SettableFuture<SendResult> future = SettableFuture.create();
			sendMsgCmd.addMessage(pmsg, future);
		}

		SendMessageCommand decodedSendMsgCmd = serializeAndDeserialize(sendMsgCmd);

		MessageQueueStorage storage = lookup(MessageQueueStorage.class, Storage.MYSQL);
		lookup(JdbcTestHelper.class).dumpTo(DATASOURCE, null, "h2_100_0_offset_message");
		storage.appendMessages(tpp, decodedSendMsgCmd.getMessageRawDataBatches().values());

		// assertTrue(latch.await(2, TimeUnit.SECONDS));
		latch.await(2, TimeUnit.SECONDS);
		lookup(JdbcTestHelper.class).dumpTo(DATASOURCE, null, "h2_100_0_message_1");

		List<TppConsumerMessageBatch> batches = pullMsgResultCmdRef.get().getBatches();
		List<ConsumerMessage> cmsgs = new ArrayList<ConsumerMessage>();
		for (TppConsumerMessageBatch batch : batches) {
			List<MessageMeta> msgMetas = batch.getMessageMetas();
			ByteBuf batchData = batch.getData();

			for (int j = 0; j < msgMetas.size(); j++) {
				BaseConsumerMessage baseMsg = lookup(MessageCodec.class).decode(batch.getTopic(), batchData, String.class);
				BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
				MessageMeta messageMeta = msgMetas.get(j);
				brokerMsg.setPartition(partition);
				brokerMsg.setPriority(messageMeta.getPriority() == 0 ? true : false);
				brokerMsg.setResend(messageMeta.isResend());
				brokerMsg.setMsgSeq(messageMeta.getId());

				cmsgs.add(brokerMsg);
			}
		}

		Map<String, ConsumerMessage> refKey2ConsumerMsg = new HashMap<>();
		for (ConsumerMessage cmsg : cmsgs) {
			refKey2ConsumerMsg.put(cmsg.getRefKey(), cmsg);
		}

		assertEquals(pmsgs.size(), cmsgs.size());
		for (ProducerMessage<String> pmsg : pmsgs) {
			assertTrue(refKey2ConsumerMsg.containsKey(pmsg.getKey()));
			assertMessageEqual(pmsg, refKey2ConsumerMsg.get(pmsg.getKey()));
		}

		// TODO assert offset
	}

}
