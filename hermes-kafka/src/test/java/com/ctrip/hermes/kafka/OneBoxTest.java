package com.ctrip.hermes.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.kafka.admin.MockKafka;
import com.ctrip.hermes.kafka.admin.MockZookeeper;
import com.ctrip.hermes.kafka.producer.KafkaFuture;
import com.ctrip.hermes.kafka.producer.KafkaMessageSender;
import com.ctrip.hermes.kafka.producer.KafkaSendResult;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.sender.MessageSender;

public class OneBoxTest {

	private MockZookeeper zk;

	private MockKafka kafka;

	@Before
	public void before() {
		zk = new MockZookeeper();
		kafka = new MockKafka();
	}

	@After
	public void after() {
		kafka.stop();
		zk.stop();
	}

	@Test
	public void simpleTextOneProducerOneConsumerTest() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic1";
		kafka.createTopic(topic);
		String group = UUID.randomUUID().toString();

		List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual = new ArrayList<String>();

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive: " + body);
			}
		});

		System.out.println("Starting consumer...");

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			if (System.currentTimeMillis() % 2 == 0) {
				holder = holder.withoutHeader();
			}
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}

		int sleepCount = 0;
		while (actual.size() < expected.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
		consumer.close();
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(expected, actual);
	}

	/**
	 * Could not guarantee the total order of consumer side
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void simpleTextOneProducerMultipleConsumerInOneGroupTest() throws IOException, InterruptedException,
	      ExecutionException {
		String topic = "kafka.SimpleTextTopic2";
		kafka.createTopic(topic);
		String group = UUID.randomUUID().toString();

		List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual = new ArrayList<String>();

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer1 = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer1");

		ConsumerHolder consumer2 = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive2: " + body);
			}
		});
		System.out.println("Starting consumer2");

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			if (System.currentTimeMillis() % 2 == 0) {
				holder = holder.withoutHeader();
			}
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}

		int sleepCount = 0;
		while (actual.size() < expected.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();

		consumer1.close();
		consumer2.close();
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));
	}

	@Test
	public void simpleTextOneProducerMultipleConsumerInMultipleGroupTest() throws IOException, InterruptedException,
	      ExecutionException {
		String topic = "kafka.SimpleTextTopic3";
		kafka.createTopic(topic);
		String group1 = UUID.randomUUID().toString();
		String group2 = UUID.randomUUID().toString();

		List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual1 = new ArrayList<String>();
		final List<String> actual2 = new ArrayList<String>();

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer1 = Consumer.getInstance().start(topic, group1, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual1.add(body);
				System.out.println("Receive: " + body);
			}
		});

		System.out.println("Starting consumer1");

		ConsumerHolder consumer2 = Consumer.getInstance().start(topic, group2, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual2.add(body);
				System.out.println("Receive: " + body);
			}
		});
		System.out.println("Starting consumer2");

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			if (System.currentTimeMillis() % 2 == 0) {
				holder = holder.withoutHeader();
			}
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}

		int sleepCount = 0;
		while ((actual1.size() < expected.size() || actual2.size() < expected.size()) && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();

		consumer1.close();
		consumer2.close();
		Assert.assertEquals(expected.size(), actual1.size());
		Assert.assertEquals(expected.size(), actual2.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual1));
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual2));
	}

	@Test
	public void simpleTextMultipleProducerOneConsumerTest() throws IOException, InterruptedException, ExecutionException {
		final String topic = "kafka.SimpleTextTopic4";
		kafka.createTopic(topic);
		final String group = UUID.randomUUID().toString();

		final List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual = new ArrayList<String>();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive: " + body);
			}
		});

		System.out.println("Starting consumer...");

		Thread producer1 = new Thread() {
			public void run() {
				Producer producer = Producer.getInstance();
				for (int i = 0; i < expected.size(); i++) {
					String proMsg = expected.get(i);

					MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
					if (System.currentTimeMillis() % 2 == 0) {
						holder = holder.withoutHeader();
					}
					KafkaFuture future = (KafkaFuture) holder.send();
					KafkaSendResult result;
					try {
						result = future.get();
						System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
						      result.getOffset()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
		};
		producer1.start();

		Thread producer2 = new Thread() {
			public void run() {
				Producer producer = Producer.getInstance();
				for (int i = 0; i < expected.size(); i++) {
					String proMsg = expected.get(i);

					MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
					if (System.currentTimeMillis() % 2 == 0) {
						holder = holder.withoutHeader();
					}
					KafkaFuture future = (KafkaFuture) holder.send();
					KafkaSendResult result;
					try {
						result = future.get();
						System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
						      result.getOffset()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
		};
		producer2.start();

		int sleepCount = 0;
		while (actual.size() < expected.size() * 2 && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();

		consumer.close();
		Assert.assertEquals(expected.size() * 2, actual.size());
	}
}
