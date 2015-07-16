package com.ctrip.hermes.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.kafka.avro.AvroVisitEvent;
import com.ctrip.hermes.kafka.avro.KafkaAvroTest;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class OneBoxInDevTest {

	@Test
	public void simpleTextMessageTest() throws IOException {
		String topic = "kafka.SimpleTextTopic";
		String group = "simpleTextMessageTest";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				System.out.println("Receive: " + body);
			}
		});

		System.out.println("Starting consumer...");

		boolean hasHeader = true;
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				String proMsg = "Hello Ctrip " + System.currentTimeMillis();
				MessageHolder holder = producer.message(topic, null, proMsg);
				if (!hasHeader) {
					holder = holder.withoutHeader();
				}
				hasHeader = !hasHeader;
				holder.send();
				System.out.println("Sent: " + proMsg);
			}
		}

		consumer.close();
	}

	@Test
	public void simpleAvroMessageTest() throws IOException {
		String topic = "kafka.SimpleAvroTopic";
		String group = "simpleTextMessageTest";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<AvroVisitEvent>() {

			@Override
			protected void onMessage(ConsumerMessage<AvroVisitEvent> msg) {
				AvroVisitEvent body = msg.getBody();
				System.out.println("Receive: " + body);
			}
		});

		System.out.println("Starting consumer...");

		boolean hasHeader = true;
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				AvroVisitEvent proMsg = KafkaAvroTest.generateEvent();
				MessageHolder holder = producer.message(topic, null, proMsg);
				if (!hasHeader) {
					holder = holder.withoutHeader();
				}
				hasHeader = !hasHeader;
				holder.send();
				System.out.println("Sent: " + proMsg);
			}
		}

		consumer.close();
	}
}
