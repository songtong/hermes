package com.ctrip.hermes.portal.service.storage;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

public interface TopicStorageService {
	public boolean initTopicStorage(Topic topic);

	public boolean dropTopicStorage(Topic topic);

	public boolean addPartitionStorage(Topic topic, Partition partition);

	public boolean delPartitionStorage(Topic topic, Partition partition);

	public boolean addConsumerStorage(Topic topic, ConsumerGroup group);

	public boolean delConsumerStorage(Topic topic, ConsumerGroup group);
}
