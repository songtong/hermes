package com.ctrip.hermes.producer;

import java.io.IOException;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.producer.api.Producer;

public class ProducerTest extends ComponentTestCase {
	@Test
	public void simpleSendWithoutLookup() throws IOException {
		Producer p = Producer.getInstance();

		p.message("order_new", "0", 123456L).withRefKey("key").send();
	}

	@Test
	public void simpleSend() {
		Producer p = lookup(Producer.class);

		p.message("local.order.new", null, 12346L).send();
	}

	@Test
	public void sendWithKey() {
		Producer p = lookup(Producer.class);

		p.message("local.order.new", null,  12347L).withRefKey("key12345").send();
	}

	@Test
	public void sendWithPriority() {
		Producer p = lookup(Producer.class);

		p.message("local.order.new", null, 12348L).withPriority().send();
	}

}
