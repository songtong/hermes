package com.ctrip.hermes.kafka.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class KafkaAvroTest {

	// @Test
	public void testByConsole() throws InterruptedException, ExecutionException, IOException {
		String topic = "kafka.AvroTopic";
		String group = "avroGroup";

		Producer producer = Producer.getInstance();

		final List<AvroVisitEvent> actualResult = new ArrayList<>();
		final List<AvroVisitEvent> expectedResult = new ArrayList<>();

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topic, group, new MessageListener<AvroVisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<AvroVisitEvent>> msgs) {
				for (ConsumerMessage<AvroVisitEvent> msg : msgs) {
					AvroVisitEvent event = msg.getBody();
					System.out.println("Consumer Received: " + event);
					actualResult.add(event);
				}
			}
		});

		System.out.println("Starting consumer...");

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				AvroVisitEvent event = generateEvent();
				MessageHolder holder = producer.message(topic, null, event);
				Future<SendResult> future = holder.send();
				future.get();
				if (future.isDone()) {
					System.out.println("Producer Sent: " + event);
					expectedResult.add(event);
				}
			}
		}
		consumerHolder.close();
		Assert.assertEquals(expectedResult.size(), actualResult.size());
	}

	@Test
	public void testByBatch() throws InterruptedException, ExecutionException, IOException {
		String topic = "kafka.AvroTopic";
		String group = "avroGroup";

		Producer producer = Producer.getInstance();

		final List<AvroVisitEvent> actualResult = new ArrayList<>();
		final List<AvroVisitEvent> expectedResult = new ArrayList<>();

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topic, group, new MessageListener<AvroVisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<AvroVisitEvent>> msgs) {
				for (ConsumerMessage<AvroVisitEvent> msg : msgs) {
					AvroVisitEvent event = msg.getBody();
					System.out.println("Consumer Received: " + event);
					actualResult.add(event);
				}
			}
		});

		System.out.println("Starting consumer...");

		int limit = new Random().nextInt(20) + 1;
		int i = 0;
		while (i++ < limit) {
			AvroVisitEvent event = generateEvent();
			MessageHolder holder = producer.message(topic, null, event);
			Future<SendResult> future = holder.send();
			future.get();
			if (future.isDone()) {
				System.out.println("Producer Sent: " + event);
				expectedResult.add(event);
			}
		}

		Assert.assertEquals(expectedResult.size(), actualResult.size());
		consumerHolder.close();
	}

	static AtomicLong counter = new AtomicLong();

	static AvroVisitEvent generateEvent() {
		Random random = new Random(System.currentTimeMillis());
		AvroVisitEvent event = AvroVisitEvent.newBuilder().setIp("192.168.0." + random.nextInt(255))
		      .setTz(System.currentTimeMillis()).setUrl("www.ctrip.com/" + counter.incrementAndGet()).build();
		return event;
	}
}
