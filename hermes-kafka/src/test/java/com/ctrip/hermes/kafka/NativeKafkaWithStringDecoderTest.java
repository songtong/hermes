package com.ctrip.hermes.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;

public class NativeKafkaWithStringDecoderTest {

	static {
		MockKafka.LOCALHOST_BROKER = "10.3.6.237:9092,10.3.6.239:9092,10.3.6.24:9092";
		MockZookeeper.ZOOKEEPER_CONNECT = "10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181";
	}

	@Test
	public void testNative() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic";
		ZkClient zkClient = new ZkClient(MockZookeeper.ZOOKEEPER_CONNECT);
		zkClient.setZkSerializer(new ZKStringSerializer());
		int msgNum = 100000;
		final CountDownLatch countDown = new CountDownLatch(msgNum);

		Properties producerProps = new Properties();
		// Producer
		producerProps.put("metadata.broker.list", MockKafka.LOCALHOST_BROKER);
		producerProps.put("bootstrap.servers", MockKafka.LOCALHOST_BROKER);
		producerProps.put("value.serializer", StringSerializer.class.getCanonicalName());
		producerProps.put("key.serializer", StringSerializer.class.getCanonicalName());
		// Consumer
		Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", MockZookeeper.ZOOKEEPER_CONNECT);
		consumerProps.put("group.id", "GROUP_" + topic);

		final List<String> actualResult = new ArrayList<>();
		final List<String> expectedResult = new ArrayList<>();

		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, 1);
		final List<KafkaStream<String, String>> streams = consumerConnector.createMessageStreams(topicCountMap,
		      new StringDecoder(null), new StringDecoder(null)).get(topic);
		for (final KafkaStream<String, String> stream : streams) {
			new Thread() {
				public void run() {
					for (MessageAndMetadata<String, String> msgAndMetadata : stream) {
						try {
							System.out.println("received: " + msgAndMetadata.message());
							actualResult.add(msgAndMetadata.message());
							countDown.countDown();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
		}

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
		int i = 0;
		while (i < msgNum) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, "test-message" + i++);
			Future<RecordMetadata> send = producer.send(data);
			send.get();
			if (send.isDone()) {
				System.out.println("sending: " + data.value());
				expectedResult.add(data.value());
			}
		}

		countDown.await();

		Assert.assertArrayEquals(expectedResult.toArray(), actualResult.toArray());

		consumerConnector.shutdown();
		producer.close();
	}
}
