package com.ctrip.hermes.collector.job.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.admin.core.model.ConsumerGroupDao;
import com.ctrip.hermes.admin.core.model.ConsumerGroupEntity;
import com.ctrip.hermes.admin.core.model.Partition;
import com.ctrip.hermes.admin.core.model.PartitionDao;
import com.ctrip.hermes.admin.core.model.PartitionEntity;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.admin.core.queue.DeadLetter;
import com.ctrip.hermes.admin.core.queue.DeadLetterDao;
import com.ctrip.hermes.admin.core.queue.DeadLetterEntity;
import com.ctrip.hermes.admin.core.queue.MessageQueueDao;
import com.ctrip.hermes.admin.core.queue.OffsetMessage;
import com.ctrip.hermes.collector.hub.StateHub;
import com.ctrip.hermes.collector.job.AbstractJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.dianping.cat.Cat;


/**
 * @author tenglinxiao
 *
 */
//@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
public class TopicConsumeCollectorJob extends AbstractJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicConsumeCollectorJob.class);
	
	private MessageQueueDao m_messageQueueDao = PlexusComponentLocator.lookup(MessageQueueDao.class);
	
	private DeadLetterDao m_deadLetterDao = PlexusComponentLocator.lookup(DeadLetterDao.class);
	
	private PartitionDao m_partitionDao = PlexusComponentLocator.lookup(PartitionDao.class);
	
	private ConsumerGroupDao m_consumerGroupDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	public void setup(JobContext context) {}

	public boolean doRun(JobContext context) throws Exception {
		long to = context.getScheduledExecutionTime().getTime();
		long from = TimeUtils.before(to, 5, TimeUnit.MINUTES);
		
		List<State> states = new ArrayList<>();
		List<Topic> topics = m_topicDao.list(TopicEntity.READSET_FULL);
		
		for (Topic topic : topics) {
			// Ignore non-mysql topic.
			if (!Storage.MYSQL.equals(topic.getStorageType())) {
				continue;
			}
			
			List<Partition> partitions = m_partitionDao.findByTopicId(topic.getId(), PartitionEntity.READSET_FULL);
			
			for (Partition partition : partitions) {
				Map<Integer, Pair<OffsetMessage, OffsetMessage>> offsets = null;
				try {
					offsets = m_messageQueueDao.getLatestConsumed(topic.getName(), partition.getId());
				} catch (Exception e) {
					LOGGER.error("Failed to find consumed offsets: {}, {}", topic.getName(), partition.getId());
				}
				
				if (offsets == null) {
					continue;
				}
				
				for (Integer consumerGroupId : offsets.keySet()) {
					try {
						String consumerGroupName = m_consumerGroupDao.findByPK(consumerGroupId, ConsumerGroupEntity.READSET_FULL).getName();
						TPGConsumeState state = new TPGConsumeState();
						Pair<OffsetMessage, OffsetMessage> offset = offsets.get(consumerGroupId);
						state.setStorageType(Storage.MYSQL);
						state.setTopicId(topic.getId());
						state.setTopicName(topic.getName());
						state.setPartition(partition.getId());
						state.setConsumerGroupId(consumerGroupId);
						state.setConsumerGroup(consumerGroupName);
						
						if (offset.getKey() != null) {
							state.setOffsetPriority(offset.getKey().getOffset());
							state.setOffsetPriorityModifiedDate(offset.getKey().getLastModifiedDate());
						}
						
						if (offset.getValue() != null){
							state.setOffsetNonPriority(offset.getValue().getOffset());
							state.setOffsetNonPriorityModifiedDate(offset.getValue().getLastModifiedDate());
						}
						
						DeadLetter deadLetter = m_deadLetterDao.countByConsumerTimeRange(topic.getName(), partition.getId(), consumerGroupId, new Date(from), new Date(to), DeadLetterEntity.READSET_COUNT);
						state.setDeadLetterCount(deadLetter.getCountOfTimeRange());
						state.setIndex(IndexUtils.getDataStoredIndex(RecordType.TOPIC_FLOW_DB.getCategory().getName(), false));
						state.setTimestamp(to);
						states.add(state);
					} catch(DalNotFoundException e) {
						LOGGER.warn("Consumer group {} not existed on topic {}, partition {}", consumerGroupId, topic.getName(), partition.getId());
					} catch (Exception e) {
						LOGGER.error("Failed to find offset message for consumer with id {} on topic {}, partition {}", consumerGroupId, topic.getName(), partition.getId(), e);
						continue;
					}
				}
			}
			
			Cat.logEvent("MysqlConsumes", topic.getName());
		}
		
		context.setStates(states);
		
		return true;
	}
	
	public void complete(JobContext context) {
		super.complete(context);
		TPGConsumeState state = new TPGConsumeState();
		state.setStorageType(Storage.MYSQL);
		state.setSync(true);
		this.getApplicationContext().getBean(StateHub.class).offer(state);
	}

}
