package com.ctrip.hermes.portal.service.storage;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

public interface TopicStorageService {
	public boolean initTopicStorage(Topic topic);

	public boolean addPartition(Topic topic, Partition partition);
}
