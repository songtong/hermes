package com.ctrip.hermes.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.kafka.admin.ZKStringSerializer;
import com.ctrip.hermes.kafka.avro.KafkaAvroTest;

public class NativeKafkaWithAvroDecoderTest {

	@Test
	public void testNative() throws IOException, InterruptedException, ExecutionException {
		final String topic = "kafka.SimpleAvroTopic";
		ZkClient zkClient = new ZkClient("10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181");
		zkClient.setZkSerializer(new ZKStringSerializer());
		int msgNum = 100000;
		final CountDownLatch countDown = new CountDownLatch(msgNum);

		Properties producerProps = new Properties();
		// Producer
		producerProps.put("metadata.broker.list", "10.3.6.237:9092,10.3.6.239:9092,10.3.6.24:9092");
		producerProps.put("bootstrap.servers", "10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181");

		// Avro Decoder/Encoder
		CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("http://10.3.8.63:8081/",
		      AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
		Map<String, String> configs = new HashMap<String, String>();
		configs.put("schema.registry.url", "http://10.3.8.63:8081/");

		KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
		avroKeySerializer.configure(configs, true);
		KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
		avroValueSerializer.configure(configs, false);

		Map<String, String> deserializerConfigs = new HashMap<String, String>();
		deserializerConfigs.put("specific.avro.reader", Boolean.TRUE.toString());
		deserializerConfigs.put("schema.registry.url", "http://10.3.8.63:8081/");
		KafkaAvroDeserializer avroKeyDeserializer = new KafkaAvroDeserializer(schemaRegistry, deserializerConfigs);
		avroKeyDeserializer.configure(configs, true);
		KafkaAvroDeserializer avroValueDeserializer = new KafkaAvroDeserializer(schemaRegistry, deserializerConfigs);
		avroValueDeserializer.configure(configs, false);

		// Consumer
		final Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", "10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181");
		consumerProps.put("group.id", "GROUP_" + topic);

		final List<Object> actualResult = new ArrayList<Object>();
		final List<Object> expectedResult = new ArrayList<Object>();

		final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(consumerProps,
		      avroKeyDeserializer, avroValueDeserializer);
		consumer.subscribe(Arrays.asList(topic));

		new Thread() {
			public void run() {
				ConsumerRecords<Object, Object> records = consumer.poll(10000);
				for (ConsumerRecord<Object, Object> consumerRecord : records) {
					try {
						System.out.println("received: " + consumerRecord.value());
						actualResult.add(consumerRecord.value());
						countDown.countDown();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}.start();

		KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(producerProps, avroKeySerializer,
		      avroValueSerializer);
		int i = 0;
		while (i++ < msgNum) {
			ProducerRecord<Object, Object> data = new ProducerRecord<Object, Object>(topic, null,
			      (Object) KafkaAvroTest.generateEvent());
			Future<RecordMetadata> send = producer.send(data);
			send.get();
			if (send.isDone()) {
				System.out.println("sending: " + data.value());
				expectedResult.add(data.value());
			}
		}

		countDown.await();

		Assert.assertArrayEquals(expectedResult.toArray(), actualResult.toArray());

		consumer.close();
		producer.close();
	}
}
