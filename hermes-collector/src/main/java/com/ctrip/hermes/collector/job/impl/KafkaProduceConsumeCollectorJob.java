package com.ctrip.hermes.collector.job.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.model.ConsumerGroup;
import com.ctrip.hermes.admin.core.model.ConsumerGroupDao;
import com.ctrip.hermes.admin.core.model.ConsumerGroupEntity;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.hub.MetricsHub;
import com.ctrip.hermes.collector.hub.StateHub;
import com.ctrip.hermes.collector.job.AbstractJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.state.impl.TPProduceState;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hickwall.proxy.common.DataPoint;

@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
public class KafkaProduceConsumeCollectorJob extends AbstractJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProduceConsumeCollectorJob.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	private ConsumerGroupDao m_consumerGroupDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);
	
	@Autowired
	private MetricsHub m_metricsHub;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	public void setup(JobContext context) {}

	@Override
	public boolean doRun(JobContext context) throws Exception {
		long to = context.getScheduledExecutionTime().getTime();
		
		List<String> brokers = Arrays.asList(m_conf.getKafkaBootstrapServers().split(","));
		List<State> states = new ArrayList<State>();

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
			Map<String, List<TopicAndPartition>> metadatas = getMetadataByLeaderHosts(findLeader(brokers, m_conf.getKafkaServerPort(), kafkaTopics));  
	        	
			for (Entry<String, List<TopicAndPartition>> metadata : metadatas.entrySet()) {	
				ArrayList<DataPoint> points = new ArrayList<>();
				
	            String clientName = "Client_" + metadata.getKey();  
	            SimpleConsumer consumer = null;
	            try {
		            consumer = new SimpleConsumer(metadata.getKey(), m_conf.getKafkaServerPort(), 100000, 64 * 1024, clientName);  
		            
		            // Check produce states.
		            Map<TopicAndPartition, Long> offsets = getLatestProudcerOffsets(consumer, metadata.getValue(), kafka.api.OffsetRequest.LatestTime(), clientName);  
		            for (Map.Entry<TopicAndPartition, Long> topicPartitionOffset : offsets.entrySet()) {
	            		Topic topic = m_topicDao.findByName(topicPartitionOffset.getKey().topic(), TopicEntity.READSET_FULL);
	            		TPProduceState state = new TPProduceState();
	            		state.setStorageType(Storage.KAFKA);
	            		state.setTopicId(topic.getId());
	            		state.setTopicName(topic.getName());
	            		state.setOffsetNonPriority(topicPartitionOffset.getValue());
	            		state.setLastNonPriorityCreationDate(new Date(to));
	            		state.setPartition(topicPartitionOffset.getKey().partition());
	            		state.setTimestamp(to);
	            		state.setIndex(IndexUtils.getDataStoredIndex(RecordType.TOPIC_FLOW_DB.getCategory().getName(), false));
	            		states.add(state);
	            		
	            		DataPoint point = new DataPoint(String.format("hermes.kafka.produce.nonpriority.%d-%d", state.getTopicId(), state.getPartition()), (double)state.getOffsetNonPriority(), TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()));
						point.setEndpoint(m_conf.getHickwallEndpoint());
						points.add(point);
					}
		            
		            m_metricsHub.send(points);
		            commitStates(states);
		            
		            // Check consumer states.
		            String topic = null;
		            List<TopicAndPartition> topicPartitions = new ArrayList<>();
		            for (TopicAndPartition topicPartition : metadata.getValue()) {
		            	if (topic != null && !topic.equals(topicPartition.topic())) {
		            		states.addAll(getLatestConsumerOffsetsOnTopic(consumer, topicPartitions, clientName, to));
		            		topicPartitions.clear();
		            	}
		            	topic = topicPartition.topic();
	            		topicPartitions.add(topicPartition);
		            }
	        		states.addAll(getLatestConsumerOffsetsOnTopic(consumer, topicPartitions, clientName, to));        		
	        		commitStates(states);
	            } catch (Exception e) {
	            	LOGGER.error("Failed to finish offset fetch on broker {}", metadata.getKey());
	            } finally {
	            	if (consumer != null) {
		            	consumer.close();  
		            }
	            }
			}
		}
		
		return true;
	}
	
	private List<State> getLatestConsumerOffsetsOnTopic(SimpleConsumer consumer, List<TopicAndPartition> topicAndPartitions, String clientName, long timestamp) throws DalException {
		Topic topic = m_topicDao.findByName(topicAndPartitions.get(0).topic(), TopicEntity.READSET_FULL);
		List<State> states = new ArrayList<State>();
    	List<ConsumerGroup> groups = m_consumerGroupDao.findByTopicId(topic.getId(), ConsumerGroupEntity.READSET_FULL);
    	ArrayList<DataPoint> points = new ArrayList<DataPoint>();
    	
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
		            
		            DataPoint point = new DataPoint(String.format("hermes.kafka.consume.nonpriority.%d-%d-%d", state.getTopicId(), state.getPartition(), state.getConsumerGroupId()), (double)state.getOffsetNonPriority(), TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()));
					point.setEndpoint(m_conf.getHickwallEndpoint());
					points.add(point);
            	}
            }
        }
        m_metricsHub.send(points);
        
        return states;
	}
	
	private Map<TopicAndPartition, Long> getLatestConsumerOffsets(SimpleConsumer consumer, List<TopicAndPartition> topicAndPartitions, String clientName, String groupId) {  
		Map<TopicAndPartition, PartitionOffsetRequestInfo> info = new HashMap<>();
		for (TopicAndPartition tp : topicAndPartitions) {
			info.put(tp, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
		}
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(info, OffsetRequest.CurrentVersion(), clientName);  
        OffsetResponse response = consumer.getOffsetsBefore(request);
        Map<TopicAndPartition, Long> tpOffsets = new HashMap<TopicAndPartition, Long>();
        
        for (TopicAndPartition tp : topicAndPartitions) {
        	if (!response.hasError()) {
        		long[] offsets = response.offsets(tp.topic(), tp.partition());
        		if (offsets != null && offsets.length > 0) {
        			tpOffsets.put(tp, offsets[0]);
        		}
        	} else {
        		LOGGER.error("Failed to fetch offset [{}, {}, {}] from broker. Reason: {}", tp.topic(), tp.partition(), groupId, response.errorCode(tp.topic(), tp.partition()));
        	}
        }
        
        return tpOffsets;
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
		
		// Sort by topic&partition.
		for (List<TopicAndPartition> topicAndPartitions : topicPartitionsByLeaders.values()) {
			Collections.sort(topicAndPartitions, new Comparator<TopicAndPartition> (){

				@Override
				public int compare(TopicAndPartition tp1, TopicAndPartition tp2) {
					int cmp = tp1.topic().compareTo(tp2.topic());
					if (cmp != 0) {
						return cmp;
					}
					
					return tp1.partition() - tp2.partition();
				}
				
			});
		}
		return topicPartitionsByLeaders;
	}
	
	private Map<TopicAndPartition, Long> getLatestProudcerOffsets(SimpleConsumer consumer, List<TopicAndPartition> topicsParitions, long whichTime, String clientName) {  
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();  
        for (TopicAndPartition topicPartition : topicsParitions) {
        	requestInfo.put(topicPartition, new PartitionOffsetRequestInfo(whichTime, 1));  
        }
       
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(  
        		requestInfo, kafka.api.OffsetRequest.CurrentVersion(),  
                clientName);  
        OffsetResponse response = consumer.getOffsetsBefore(request);  
  
        if (response.hasError()) {  
            LOGGER.error("Error fetching data offset from the broker {}.", consumer.host());  
            return null;  
        }  
        
        Map<TopicAndPartition, Long> topicsPartitionsOffsets = new HashMap<>();
        
        for (TopicAndPartition topicPartition : topicsParitions) {
        		long[] offsets = response.offsets(topicPartition.topic(), topicPartition.partition());
        		topicsPartitionsOffsets.put(topicPartition, offsets[0]);
        }
          
        return topicsPartitionsOffsets;  
    }   
  
	private Map<String, TreeMap<Integer,PartitionMetadata>> findLeader(List<String> brokers,  
            int port, List<String> topics) { 
        Map<String, TreeMap<Integer, PartitionMetadata>> topicPartitions = new HashMap<>();  
        Collections.shuffle(brokers);
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
		TPProduceState produceState = new TPProduceState();
		produceState.setStorageType(Storage.KAFKA);
		produceState.setSync(true);
		produceState.setTimestamp(context.getScheduledExecutionTime().getTime());
		
		this.getApplicationContext().getBean(StateHub.class).offer(produceState);
		
		TPGConsumeState consumeState = new TPGConsumeState();
		consumeState.setStorageType(Storage.KAFKA);
		consumeState.setSync(true);
		consumeState.setTimestamp(context.getScheduledExecutionTime().getTime());
		
		this.getApplicationContext().getBean(StateHub.class).offer(consumeState);
	}

}
