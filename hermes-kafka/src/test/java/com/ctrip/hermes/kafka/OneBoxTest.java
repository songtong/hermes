package com.ctrip.hermes.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.kafka.avro.AvroVisitEvent;
import com.ctrip.hermes.kafka.avro.KafkaAvroTest;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class OneBoxTest {

	@Test
	public void simpleTextMessageTest() throws IOException {
		String topic = "kafka.OneBox";
		String group = "group" + RandomStringUtils.randomAlphabetic(5);

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>(group) {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				System.out.println("Receive: " + body);
			}
		});

		System.out.println("Starting consumer...");

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				String proMsg = RandomStringUtils.randomAlphanumeric(10) + System.currentTimeMillis();
				MessageHolder holder = producer.message(topic, null, proMsg);
				holder.send();
				System.out.println("Sent: " + proMsg);
			}
		}

		consumer.close();
	}

	@Test
	public void simpleAvroMessageTest() throws IOException {
		String topic = "kafka.AvroTopic";
		String group = "group" + RandomStringUtils.randomAlphabetic(5);

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group,
		      new BaseMessageListener<AvroVisitEvent>(group) {

			      @Override
			      protected void onMessage(ConsumerMessage<AvroVisitEvent> msg) {
				      AvroVisitEvent body = msg.getBody();
				      System.out.println("Receive: " + body);
			      }
		      });

		System.out.println("Starting consumer...");

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				AvroVisitEvent proMsg = KafkaAvroTest.generateEvent();
				MessageHolder holder = producer.message(topic, null, proMsg);
				holder.send();
				System.out.println("Sent: " + proMsg);
			}
		}

		consumer.close();
	}
}
