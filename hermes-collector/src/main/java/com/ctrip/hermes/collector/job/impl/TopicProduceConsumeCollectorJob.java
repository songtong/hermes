package com.ctrip.hermes.collector.job.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.tuple.Pair;

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
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.model.Partition;
import com.ctrip.hermes.metaservice.model.PartitionDao;
import com.ctrip.hermes.metaservice.model.PartitionEntity;
import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.ctrip.hermes.metaservice.queue.DeadLetter;
import com.ctrip.hermes.metaservice.queue.DeadLetterDao;
import com.ctrip.hermes.metaservice.queue.DeadLetterEntity;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessageQueueDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
public class TopicProduceConsumeCollectorJob extends AbstractJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicProduceConsumeCollectorJob.class);
	
	private MessageQueueDao m_messageQueueDao = PlexusComponentLocator.lookup(MessageQueueDao.class);
	
	private PartitionDao m_partitionDao = PlexusComponentLocator.lookup(PartitionDao.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	private ConsumerGroupDao m_consumerGroupDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);
	
	private DeadLetterDao m_deadLetterDao = PlexusComponentLocator.lookup(DeadLetterDao.class);
	
	public void setup(JobContext context) {}

	@Override
	public boolean doRun(JobContext context) throws Exception {
		long to = context.getScheduledExecutionTime().getTime();
		long from = TimeUtils.before(to, 5, TimeUnit.MINUTES);
		
		long produceSuccess = 0;
		long produceFail = 0;
		long consumeSuccess = 0;
		long consumeFail = 0;
		
		List<State> states = new ArrayList<>();
		List<Topic> topics = m_topicDao.list(TopicEntity.READSET_FULL);
		
		for (int index = 0; index < topics.size(); index++) {
			Topic topic = topics.get(index);
			
			// Ignore non-mysql topic.
			if (!Storage.MYSQL.equals(topic.getStorageType())) {
				continue;
			}
			
			List<Partition> partitions = m_partitionDao.findByTopicId(topic.getId(), PartitionEntity.READSET_FULL);
			
			for (Partition partition : partitions) {				
				// Find produce state on partitions.
				Transaction produce = Cat.newTransaction("Mysql", "CollectProduceOnPartition");
				TPProduceState produceState = new TPProduceState();
				try {
					produceState.setStorageType(Storage.MYSQL);
					produceState.setTopicId(topic.getId());
					produceState.setTopicName(topic.getName());
					produceState.setPartition(partition.getId());

					// Priority message.
					MessagePriority message = m_messageQueueDao.getLatestProduced(topic.getName(), partition.getId(), 0);
					if (message != null) {
						produceState.setOffsetPriority(message.getId());
						produceState.setLastPriorityCreationDate(message.getCreationDate());
					}
					// Non-priority message.
					message = m_messageQueueDao.getLatestProduced(topic.getName(), partition.getId(), 1);
					if (message != null) {
						produceState.setOffsetNonPriority(message.getId());
						produceState.setLastNonPriorityCreationDate(message.getCreationDate());
					}
					
					produceState.setIndex(IndexUtils.getDataStoredIndex(RecordType.TOPIC_FLOW_DB.getCategory().getName(), false));
					produceState.setTimestamp(to);
					produce.setStatus(Message.SUCCESS);
					produceSuccess++;
				} catch (Exception e) {
					LOGGER.error("Check topic produce failed: {}, {}", topic.getName(), partition.getId(), e);
					produce.setStatus(e);
					produceFail++;
					continue;
				} finally {
					produce.complete();
				}
				states.add(produceState);
				
				// Find consume state on partitions & consumers.
				Map<Integer, Pair<OffsetMessage, OffsetMessage>> offsets = null;
				Transaction consume = Cat.newTransaction("Mysql", "CollectConsumeOnPartition");
				try {
					offsets = m_messageQueueDao.getLatestConsumed(topic.getName(), partition.getId());
					for (Integer consumerGroupId : offsets.keySet()) {
						try {
							String consumerGroupName = m_consumerGroupDao.findByPK(consumerGroupId, ConsumerGroupEntity.READSET_FULL).getName();
							TPGConsumeState consumeState = new TPGConsumeState();
							Pair<OffsetMessage, OffsetMessage> offset = offsets.get(consumerGroupId);
							consumeState.setStorageType(Storage.MYSQL);
							consumeState.setTopicId(topic.getId());
							consumeState.setTopicName(topic.getName());
							consumeState.setPartition(partition.getId());
							consumeState.setConsumerGroupId(consumerGroupId);
							consumeState.setConsumerGroup(consumerGroupName);
							
							if (offset.getKey() != null) {
								consumeState.setOffsetPriority(offset.getKey().getOffset());
								consumeState.setOffsetPriorityModifiedDate(offset.getKey().getLastModifiedDate());
							}
							
							if (offset.getValue() != null){
								consumeState.setOffsetNonPriority(offset.getValue().getOffset());
								consumeState.setOffsetNonPriorityModifiedDate(offset.getValue().getLastModifiedDate());
							}
							
							DeadLetter deadLetter = m_deadLetterDao.countByConsumerTimeRange(topic.getName(), partition.getId(), consumerGroupId, new Date(from), new Date(to), DeadLetterEntity.READSET_COUNT);
							consumeState.setDeadLetterCount(deadLetter.getCountOfTimeRange());
							consumeState.setIndex(IndexUtils.getDataStoredIndex(RecordType.TOPIC_FLOW_DB.getCategory().getName(), false));
							consumeState.setTimestamp(to);
							states.add(consumeState);
							consumeSuccess++;
						} catch(DalNotFoundException e) {
							LOGGER.warn("Consumer group {} not existed on topic {}, partition {}", consumerGroupId, topic.getName(), partition.getId());
							consumeFail++;
						} catch (Exception e) {
							LOGGER.error("Failed to find offset message for consumer with id {} on topic {}, partition {}", consumerGroupId, topic.getName(), partition.getId(), e);
							consumeFail++;
							continue;
						}
					}
					consume.setStatus(Message.SUCCESS);
				} catch (Exception e) {
					LOGGER.error("Failed to find consumed offsets: {}, {}", topic.getName(), partition.getId());
					consume.setStatus(e);
					continue;
				} finally {
					consume.complete();
				}
			}
			
			commitStates(states);
		}
		
		LOGGER.info("Mysql produces: {}, {}, Mysql consumes: {}, {}", produceSuccess, produceFail, consumeSuccess, consumeFail);
				
		return true;
	}
	
	public void complete(JobContext context) {
		super.complete(context);
		TPProduceState produceState = new TPProduceState();
		produceState.setStorageType(Storage.MYSQL);
		produceState.setSync(true);
		produceState.setTimestamp(context.getScheduledExecutionTime().getTime());
		this.getApplicationContext().getBean(StateHub.class).offer(produceState);
		
		TPGConsumeState consumeState = new TPGConsumeState();
		consumeState.setStorageType(Storage.MYSQL);
		consumeState.setSync(true);
		consumeState.setTimestamp(context.getScheduledExecutionTime().getTime());
		this.getApplicationContext().getBean(StateHub.class).offer(consumeState);
	}
}
