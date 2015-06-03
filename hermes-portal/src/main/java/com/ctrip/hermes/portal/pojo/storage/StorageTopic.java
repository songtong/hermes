package com.ctrip.hermes.portal.pojo.storage;

import java.util.List;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Topic;

public class StorageTopic {

	Topic topic;
	Datasource datasource;
	List<StoragePartition> storagePartitions;

	public StorageTopic(Topic topic, Datasource datasource, List<StoragePartition> storagePartitions) {
		this.topic = topic;
		this.datasource = datasource;
		this.storagePartitions = storagePartitions;
	}

	public Topic getTopic() {
		return topic;
	}

	public void setTopic(Topic topic) {
		this.topic = topic;
	}

	public Datasource getDatasource() {
		return datasource;
	}

	public void setDatasource(Datasource datasource) {
		this.datasource = datasource;
	}

	public List<StoragePartition> getStoragePartitions() {
		return storagePartitions;
	}

	public void setStoragePartitions(List<StoragePartition> storagePartitions) {
		this.storagePartitions = storagePartitions;
	}
}
