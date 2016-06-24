package com.ctrip.hermes.portal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;


public class KafkaOffset { 
	private static AtomicInteger generator = new AtomicInteger(0);
	  
    public static void main(String[] args) throws Exception {  
        // 读取kafka最新数据  
//         Properties props = new Properties();  
//         props.put("zookeeper.connect",  
//         "192.168.6.18:2181,192.168.6.20:2181,192.168.6.44:2181,192.168.6.237:2181,192.168.6.238:2181/kafka-zk");  
//         props.put("zk.connectiontimeout.ms", "1000000");  
//         props.put("group.id", "dirk_group");  
          
         //ConsumerConfig consumerConfig = new ConsumerConfig(props);  
         //ConsumerConnector connector = Consumer.createJavaConsumerConnector(consumerConfig);  
         
         
//        CuratorFramework zkClient = null;
//       ZKConfig conf = new ZKConfig();
//       Builder builder = CuratorFrameworkFactory.builder();
//
// 		builder.connectionTimeoutMs(conf.getZkConnectionTimeoutMillis());
// 		builder.connectString("10.2.7.137:2181,10.2.7.138:2181,10.2.7.139:2181");
// 		builder.maxCloseWaitMs(conf.getZkCloseWaitMillis());
// 		//builder.namespace(conf.getZkNamespace());
// 		builder.retryPolicy(new RetryNTimes(conf.getZkRetries(), conf.getSleepMsBetweenRetries()));
// 		builder.sessionTimeoutMs(conf.getZkSessionTimeoutMillis());
// 		builder.threadFactory(HermesThreadFactory.create("MetaService-Zk", true));
//
// 		zkClient = builder.build();
// 		zkClient.start();
// 		zkClient.blockUntilConnected();
 		
// 		GetChildrenBuilder childrenBuilder = zkClient.getChildren();
// 		try {
//	 		List<String> children = childrenBuilder.watched().forPath("/consumers/fx.cat.test/offsets/fx.cat.log.booking");
//	 		for (String child : children) {
//	 			System.out.println(child);
//	 			//byte[] bytes = zkClient.getData().forPath("/consumers/fx.cat.test/offsets/fx.cat.log.booking/" + child);
//	 			//System.out.println(new String(bytes));
//	 		}
// 		} catch (Exception e) {
// 			e.printStackTrace();
// 			
// 		}
 	 		
 		String topic = "fx.cat.log.booking";
 		String[] brokers = new String[]{"10.2.7.70", "10.2.7.71", "10.2.7.72", "10.2.7.73", "10.2.7.74" };
        int port = 9092;   
        List<String> seeds = new ArrayList<String>();  
        for (String broker : brokers) {
        	seeds.add(broker);  
        }
        KafkaOffset kot = new KafkaOffset();  
  
        TreeMap<Integer, PartitionMetadata> metadatas = kot.findLeader(seeds, port, topic); 
          
        long sum = 0;  
          
        for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {  
            int partition = entry.getKey();  
	            String leadBroker = entry.getValue().leader().host();  
	            String clientName = "Client_" + topic + "_" + partition;  
	            //String clientName = "fx.cat.log";
	            SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000,  
	                    64 * 1024, clientName);  
	            long readOffset = getLastOffset(consumer, topic, partition,  
	                    kafka.api.OffsetRequest.LatestTime(), clientName);  
	            sum += readOffset;
	            
	            findConsumerOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName, "fx.cat.test");
	            System.out.println(partition+":"+readOffset);  
	            if(consumer!=null)consumer.close();  
	            System.out.println("-----");
            
        }  
        System.out.println("总和："+sum);  
        
        
  
    }  
    
   
  
    public KafkaOffset() {  
//      m_replicaBrokers = new ArrayList<String>();  
    }  
  
//  private List<String> m_replicaBrokers = new ArrayList<String>();  
  
    public static long getLastOffset(SimpleConsumer consumer, String topic,  
            int partition, long whichTime, String clientName) {  
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,  
                partition);  
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();  
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(  
                whichTime, 1));  
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(  
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),  
                clientName);  
        OffsetResponse response = consumer.getOffsetsBefore(request);
  
        if (response.hasError()) {  
            System.out  
                    .println("Error fetching data Offset Data the Broker. Reason: "  
                            + response.errorCode(topic, partition));  
            return 0;  
        }  
        long[] offsets = response.offsets(topic, partition); 
//      long[] offsets2 = response.offsets(topic, 3);  
        return offsets[0];  
    } 
    
    public static long findConsumerOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName, String groupId) {
    	OffsetFetchRequest request = new OffsetFetchRequest(groupId, Arrays.asList(new TopicAndPartition(topic, partition)), 
    			kafka.api.OffsetRequest.CurrentVersion(), generator.incrementAndGet(), clientName);
    	OffsetFetchResponse response = consumer.fetchOffsets(request);
    	 Map<TopicAndPartition, OffsetMetadataAndError> offsets = response.offsets();
    	 for (Map.Entry<TopicAndPartition, OffsetMetadataAndError> offset : offsets.entrySet()) {
    		 System.out.println("read:" + offset.getKey().partition() + ":" + offset.getValue().offset());
    	 }
    	 return 0;
    }
  
    private TreeMap<Integer,PartitionMetadata> findLeader(List<String> a_seedBrokers,  
            int a_port, String a_topic) {  
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();  
        for (String seed : a_seedBrokers) {  
            SimpleConsumer consumer = null;  
            try {  
                consumer = new SimpleConsumer(seed, a_port, 100000, 2 * 1024 * 1024,  
                        "leaderLookup"+new Date().getTime());  
                List<String> topics = Collections.singletonList(a_topic);  
                TopicMetadataRequest req = new TopicMetadataRequest(topics);  
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);  
  
                List<TopicMetadata> metaData = resp.topicsMetadata();  
                for (TopicMetadata item : metaData) {  
                    for (PartitionMetadata part : item.partitionsMetadata()) {  
                        map.put(part.partitionId(), part);  
//                      if (part.partitionId() == a_partition) {  
//                          returnMetaData = part;  
//                          break loop;  
//                      }  
                    }  
                }  
            } catch (Exception e) {  
                System.out.println("Error communicating with Broker [" + seed  
                        + "] to find Leader for [" + a_topic + ", ] Reason: " + e);  
            } finally {  
                if (consumer != null)  
                    consumer.close();  
            }  
        }  
//      if (returnMetaData != null) {  
//          m_replicaBrokers.clear();  
//          for (kafka.cluster.Broker replica : returnMetaData.replicas()) {  
//              m_replicaBrokers.add(replica.host());  
//          }  
//      }  
        return map;  
    }  
  
}  
