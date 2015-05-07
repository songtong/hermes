package com.ctrip.hermes.consumer.api;

import java.io.IOException;

import org.junit.Test;

import com.ctrip.hermes.core.message.ConsumerMessage;

public class ConsumerTest {

	@Test
	public void test() throws IOException {
		Consumer.getInstance().start("order_new", "group1", new BaseMessageListener<String>("group1") {

			@Override
			protected void consume(ConsumerMessage<String> msg) {
				System.out.println(msg.getBody());
			}
		});

		System.in.read();
	}

}
