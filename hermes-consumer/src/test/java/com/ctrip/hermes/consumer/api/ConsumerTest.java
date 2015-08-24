package com.ctrip.hermes.consumer.api;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

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
		List<MessageStream<String>> streams = Consumer.getInstance().createMessageStreams("order_new", "group1");
		for (MessageStream<String> s : streams) {
			processStream(s);
		}
	}

	private void processStream(MessageStream<String> s) {
		long curOffset = s.getOffsetByTime(Long.MAX_VALUE);
		while (true) {
			List<ConsumerMessage<String>> msgs = s.fetchMessages(curOffset, 10);
			// process msgs
			curOffset = msgs.get(msgs.size() - 1).getOffset() + 1;
		}
	}

}
