package com.ctrip.hermes.portal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class MainTest {
	public void findAll() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.2.7.71:9092");
		props.put("group.id", "fx.cat.test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

		//consumer.subscribe(Arrays.asList("fx.cat.log.booking"));
		
		List<TopicPartition> ps = new ArrayList<TopicPartition>(); 
		Map<String, List<PartitionInfo>> partitions = consumer.listTopics();
		for (PartitionInfo p : partitions.get("fx.cat.log.booking")) {
			ps.add(new TopicPartition("fx.cat.log.booking", p.partition()));
		}
		
		System.out.println(ps.size());
		
		consumer.assign(ps);
		
		//for (String topic : partitions.keySet()) {
//			for (PartitionInfo p : consumer.partitionsFor("fx.cat.log.booking")) {
//				TopicPartition tp = new TopicPartition("fx.cat.log.booking", p.partition());
//				long position = consumer.position(tp);
//				System.out.println(p.partition() + ":" + position);
//			}
		//}
		
		for (TopicPartition tp : ps) {
			long position = consumer.position(tp);
			System.out.println(position);
		}
		
		System.out.println("----");
		
//		ConsumerRecords<String, String> records = consumer.poll(1000);
//		for (ConsumerRecord<String, String> record : records) {
//			System.out.println(record);
//		}
//		consumer.commitSync();
		
		int index = 10;
		
		while (true) {
			ConsumerRecords<String, byte[]> records = consumer.poll(1000);
			for (ConsumerRecord<String, byte[]> record : records) {
				System.out.println(record.partition() + ":" +record.offset());
			}
			consumer.commitSync();
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (--index == 0) {
				break;
			}
			
		}
	
		consumer.close();
	}

	public static void main(String args[]) {
		new MainTest().findAll();
	}
}
