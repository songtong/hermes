package com.ctrip.hermes.admin.core.service.storage.pojo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.admin.core.model.Topic;

public class StorageTopic {

	Topic topic;

	Map<String /* ds name */, Collection<StorageTable>/* tables info */> storageTables;

	public StorageTopic(Topic topic) {
		this.topic = topic;
	}

	public void addInfo(String dsName, Collection<StorageTable> tables) {
		if (null == storageTables) {
			storageTables = new HashMap<>();
		}
		storageTables.put(dsName, tables);
	}

	@Override
	public String toString() {
		return "StorageTopic{" + "topic=" + topic + ", storageTables=" + storageTables + '}';
	}
}
