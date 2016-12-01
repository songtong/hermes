package com.ctrip.hermes.collector.job.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import kafka.api.OffsetRequest;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.model.ConsumerGroup;
import com.ctrip.hermes.admin.core.model.ConsumerGroupDao;
import com.ctrip.hermes.admin.core.model.ConsumerGroupEntity;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.collector.hub.StateHub;
import com.ctrip.hermes.collector.job.AbstractJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;

//@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
public class KafkaConsumeCollectorJob extends AbstractJob {
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumeCollectorJob.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	private ConsumerGroupDao m_consumerGroupDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);
	
	public void setup(JobContext context) {}
	
	@Override
	public boolean doRun(JobContext context) throws Exception {
		long to = context.getScheduledExecutionTime().getTime();
		
		List<String> brokers = Arrays.asList(m_conf.getKafkaBootstrapServers().split(","));
		List<Topic> topics = m_topicDao.list(TopicEntity.READSET_FULL);
		List<String> kafkaTopics = new ArrayList<String>();
		
		for (Topic topic : topics) {
			// Ignore mysql topics.
			if (topic.getStorageType().equals(Storage.MYSQL)) {
				continue;
			}
			kafkaTopics.add(topic.getName());
		}
		
		if (kafkaTopics.size() > 0) {
			List<State> states = new ArrayList<>();
			Map<String, List<TopicAndPartition>> metadatas = getMetadataByLeaderHosts(findLeader(brokers, m_conf.getKafkaServerPort(), kafkaTopics));  
        	
			for (Entry<String, List<TopicAndPartition>> metadata : metadatas.entrySet()) {				
	            String clientName = "Client_" + metadata.getKey();  
	            SimpleConsumer consumer = new SimpleConsumer(metadata.getKey(), m_conf.getKafkaServerPort(), 100000, 64 * 1024, clientName); 
	            String topic = null;
	            List<TopicAndPartition> topicPartitions = new ArrayList<>();
	            for (TopicAndPartition topicPartition : metadata.getValue()) {
	            	if (topic == null || topic.equals(topicPartition.topic())) {
	            		topic = topicPartition.topic();
	            	} else {
	            		states.addAll(getLatestConsumerOffsetsOnTopic(consumer, topicPartitions, clientName, to));
	            		topicPartitions.clear();
	            	}
            		topicPartitions.add(topicPartition);
	            }
        		states.addAll(getLatestConsumerOffsetsOnTopic(consumer, topicPartitions, clientName, to));
			}
			
			context.setStates(states);
		}
		
		return true;
	}
	
	public List<State> getLatestConsumerOffsetsOnTopic(SimpleConsumer consumer, List<TopicAndPartition> topicAndPartitions, String clientName, long timestamp) throws DalException {
		Topic topic = m_topicDao.findByName(topicAndPartitions.get(0).topic(), TopicEntity.READSET_FULL);
		List<State> states = new ArrayList<State>();
    	List<ConsumerGroup> groups = m_consumerGroupDao.findByTopicId(topic.getId(), ConsumerGroupEntity.READSET_FULL);
        for (ConsumerGroup group : groups) {
            Map<TopicAndPartition, Long> offsets = getLatestConsumerOffsets(consumer, topicAndPartitions, clientName, group.getName());  
            if (offsets.size() > 0) {
            	for (Map.Entry<TopicAndPartition, Long> offset : offsets.entrySet()) {
		            TPGConsumeState state = new TPGConsumeState();
		            state.setStorageType(Storage.KAFKA);
		            state.setTopicId(topic.getId());
		            state.setTopicName(topic.getName());
		            state.setConsumerGroupId(group.getId());
		            state.setConsumerGroup(group.getName());
		            state.setOffsetNonPriority(offset.getValue());
		            state.setOffsetNonPriorityModifiedDate(new Date(timestamp));
		            state.setPartition(offset.getKey().partition());
		            state.setTimestamp(timestamp);
		            state.setIndex(IndexUtils.getDataStoredIndex(RecordType.TOPIC_FLOW_DB.getCategory().getName(), false));
		            states.add(state);
            	}
            }
        }
        return states;
	}
	
	private Map<TopicAndPartition, Long> getLatestConsumerOffsets(SimpleConsumer consumer, List<TopicAndPartition> topicAndPartitions, String clientName, String groupId) {  
        kafka.javaapi.OffsetFetchRequest request = new kafka.javaapi.OffsetFetchRequest(groupId, 
        		topicAndPartitions, OffsetRequest.CurrentVersion(), 1, clientName);  
        OffsetFetchResponse response = consumer.fetchOffsets(request);
        Map<TopicAndPartition, Long> offsets = new HashMap<TopicAndPartition, Long>();
        
        for (TopicAndPartition tp : topicAndPartitions) {
        	if (response.offsets().get(tp).error() == 0) {
        		offsets.put(tp, response.offsets().get(tp).offset());
        	} else {
        		LOGGER.error("Failed to fetch offset [{}, {}, {}] from broker. Reason: {}", tp.topic(), tp.partition(), groupId, response.offsets().get(tp).error());
        	}
        }
        
        return offsets;
    }   
	
	private Map<String, List<TopicAndPartition>> getMetadataByLeaderHosts(Map<String, TreeMap<Integer,PartitionMetadata>> topicsPartitions) {
		Map<String, List<TopicAndPartition>> topicPartitionsByLeaders = new HashMap<>();
		for (Map.Entry<String, TreeMap<Integer,PartitionMetadata>> topicPartitions : topicsPartitions.entrySet()) {
			for (Map.Entry<Integer, PartitionMetadata> topicPartition : topicPartitions.getValue().entrySet()) {
				if (!topicPartitionsByLeaders.containsKey(topicPartition.getValue().leader().host())) {
					topicPartitionsByLeaders.put(topicPartition.getValue().leader().host(), new ArrayList<TopicAndPartition>());
				}
				
				topicPartitionsByLeaders.get(topicPartition.getValue().leader().host()).add(new TopicAndPartition(topicPartitions.getKey(), topicPartition.getKey()));
			}
		}
		return topicPartitionsByLeaders;
	}
  
	private Map<String, TreeMap<Integer,PartitionMetadata>> findLeader(List<String> brokers,  
            int port, List<String> topics) { 
        Map<String, TreeMap<Integer, PartitionMetadata>> topicPartitions = new HashMap<>();  
        for (String broker : brokers) {  
            SimpleConsumer consumer = null;  
            try {  
                consumer = new SimpleConsumer(broker, port, 100000, 2 * 1024 * 1024, "leaderLookup");  
                TopicMetadataRequest req = new TopicMetadataRequest(topics);  
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);  
  
                List<TopicMetadata> topicsMetadata = resp.topicsMetadata(); 
                for (TopicMetadata topicMetadata : topicsMetadata) {
                	TreeMap<Integer, PartitionMetadata> partitions = new TreeMap<Integer, PartitionMetadata>();
                    for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {  
                    	partitions.put(partitionMetadata.partitionId(), partitionMetadata);  
                    } 
                    topicPartitions.put(topicMetadata.topic(), partitions);
                }  
                break;
            } catch (Exception e) {  
                LOGGER.error("Error communicating with Broker {} to find Leader, Reason: {}", broker, e.getMessage(), e);  
            } finally {  
                if (consumer != null) {
                    consumer.close();  
                }
            }
        }
        
        return topicPartitions;  
    }  
	
	public void complete(JobContext context) {
		super.complete(context);
		TPGConsumeState state = new TPGConsumeState();
		state.setStorageType(Storage.KAFKA);
		state.setSync(true);
		this.getApplicationContext().getBean(StateHub.class).offer(state);
	}

}
