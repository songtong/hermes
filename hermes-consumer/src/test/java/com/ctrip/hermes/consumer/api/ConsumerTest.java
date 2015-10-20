package com.ctrip.hermes.consumer.api;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.ctrip.hermes.consumer.api.Consumer.MessageStreamHolder;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class ConsumerTest {

	@Test
	public void test() throws IOException {
		Consumer.getInstance().start("order_new", "group1", new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				System.out.println(msg.getBody());
			}
		});

		System.in.read();
	}

	@Test
	public void testCreateMessageStreams() {
		MessageStreamHolder<String> holder = Consumer.getInstance().openMessageStreams("order_new", "group1",
		      String.class, null, null);
		for (MessageStream<String> s : holder.getStreams()) {
			processStream(s);
		}
	}

	private void processStream(MessageStream<String> s) {
		@SuppressWarnings("unused")
		Map<Integer, MessageStreamOffset> curOffset = Consumer.getInstance().getOffsetByTime("order_new", Long.MAX_VALUE);
		for (ConsumerMessage<String> msg : s) {
			System.out.println(msg.getBody());
		}
	}

}
