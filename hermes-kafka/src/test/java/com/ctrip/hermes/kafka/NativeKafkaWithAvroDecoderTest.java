package com.ctrip.hermes.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

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
import kafka.utils.VerifiableProperties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.kafka.avro.KafkaAvroTest;

public class NativeKafkaWithAvroDecoderTest {
	static {
		MockKafka.LOCALHOST_BROKER = "10.3.6.237:9092,10.3.6.239:9092,10.3.6.24:9092";
		MockZookeeper.ZOOKEEPER_CONNECT = "10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181";
	}

	@Test
	public void testNative() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleAvroTopic";
		ZkClient zkClient = new ZkClient(MockZookeeper.ZOOKEEPER_CONNECT);
		zkClient.setZkSerializer(new ZKStringSerializer());
		int msgNum = 100000;
		final CountDownLatch countDown = new CountDownLatch(msgNum);

		Properties produerProps = new Properties();
		// Producer
		produerProps.put("metadata.broker.list", MockKafka.LOCALHOST_BROKER);
		produerProps.put("bootstrap.servers", MockKafka.LOCALHOST_BROKER);

		// Avro Decoder/Encoder
		CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("http://10.3.8.63:8081/",
		      AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
		Map<String, String> configs = new HashMap<>();
		configs.put("schema.registry.url", "http://10.3.8.63:8081/");

		KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
		avroKeySerializer.configure(configs, true);
		KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
		avroValueSerializer.configure(configs, false);

		Properties specificDecoderProps = new Properties();
		specificDecoderProps.put("specific.avro.reader", Boolean.TRUE.toString());
		specificDecoderProps.put("schema.registry.url", "http://10.3.8.63:8081/");
		KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(schemaRegistry,
		      new VerifiableProperties(specificDecoderProps));

		// Consumer
		Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", MockZookeeper.ZOOKEEPER_CONNECT);
		consumerProps.put("group.id", "GROUP_" + topic);

		final List<Object> actualResult = new ArrayList<>();
		final List<Object> expectedResult = new ArrayList<>();

		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, 1);
		final List<KafkaStream<Object, Object>> streams = consumerConnector.createMessageStreams(topicCountMap,
		      avroDecoder, avroDecoder).get(topic);
		for (final KafkaStream<Object, Object> stream : streams) {
			new Thread() {
				public void run() {
					for (MessageAndMetadata<Object, Object> msgAndMetadata : stream) {
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

		KafkaProducer<Object, Object> producer = new KafkaProducer<>(produerProps, avroKeySerializer, avroValueSerializer);
		int i = 0;
		while (i++ < msgNum) {
			ProducerRecord<Object, Object> data = new ProducerRecord<>(topic, null, (Object) KafkaAvroTest.generateEvent());
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
