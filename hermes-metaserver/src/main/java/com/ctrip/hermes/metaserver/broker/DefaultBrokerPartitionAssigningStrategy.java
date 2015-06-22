package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerPartitionAssigningStrategy.class)
public class DefaultBrokerPartitionAssigningStrategy implements BrokerPartitionAssigningStrategy {

	@Override
	public Map<Integer, Map<String, ClientContext>> assign(List<Partition> partitions,
	      Map<String, ClientContext> brokers, Map<Integer, Map<String, ClientContext>> originAssignment) {
		Map<Integer, Map<String, ClientContext>> result = new HashMap<>();

		if (partitions == null || partitions.isEmpty() || brokers == null || brokers.isEmpty()) {
			return result;
		}
		ArrayList<Entry<String, ClientContext>> brokerEntries = new ArrayList<>(brokers.entrySet());
		int brokerCount = brokerEntries.size();

		for (Partition partition : partitions) {
			result.put(partition.getId(), new HashMap<String, ClientContext>());
		}

		int brokerPos = 0;
		for (Partition partition : partitions) {
			Entry<String, ClientContext> entry = brokerEntries.get(brokerPos);
			result.get(partition.getId()).put(entry.getKey(), entry.getValue());
			brokerPos = (brokerPos + 1) % brokerCount;
		}

		return result;
	}
}
