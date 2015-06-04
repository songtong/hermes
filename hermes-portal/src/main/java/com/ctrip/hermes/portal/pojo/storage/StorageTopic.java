package com.ctrip.hermes.portal.pojo.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.Topic;

public class StorageTopic {

	Topic topic;
	Map<String /*ds name*/, List<StorageTable>/*tables info*/> storageTables;


	public StorageTopic(Topic topic) {
		this.topic = topic;
	}

	public void addInfo(String dsName, List<StorageTable> tables) {
		if (null == storageTables) {
			storageTables = new HashMap<>();
		}
		storageTables.put(dsName, tables);
	}

	@Override
	public String toString() {
		return "StorageTopic{" +
				  "topic=" + topic +
				  ", storageTables=" + storageTables +
				  '}';
	}
}
