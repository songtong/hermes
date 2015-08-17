package com.ctrip.hermes.metaserver.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseAllocatorLocator.class)
public class DefaultConsumerLeaseAllocatorLocator implements ConsumerLeaseAllocatorLocator {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerLeaseAllocatorLocator.class);

	@Inject
	private MetaHolder m_metaHolder;

	@Inject(value = BuildConstants.LEASE_ALLOCATOR_ORDERED_CONSUME)
	private ConsumerLeaseAllocator m_orderedConsumeStrategy;

	@Inject(value = BuildConstants.LEASE_ALLOCATOR_NON_ORDERED_CONSUME)
	private ConsumerLeaseAllocator m_nonOrderedConsumeStrategy;

	@Override
	public ConsumerLeaseAllocator findAllocator(String topicName, String consumerGroupName) {
		ConsumerGroup consumerGroup = getConsumerGroup(topicName, consumerGroupName);
		if (consumerGroup != null && consumerGroup.isOrderedConsume()) {
			return m_orderedConsumeStrategy;
		} else {
			if (consumerGroup == null) {
				log.warn("ConsumerGroup {} not found for topic {}", topicName, consumerGroupName);
				return null;
			} else {
				return m_nonOrderedConsumeStrategy;
			}
		}
	}

	private ConsumerGroup getConsumerGroup(String topicName, String consumerGroupName) {
		Topic topic = m_metaHolder.getMeta().findTopic(topicName);
		if (topic != null) {
			return topic.findConsumerGroup(consumerGroupName);
		}

		return null;
	}

}
