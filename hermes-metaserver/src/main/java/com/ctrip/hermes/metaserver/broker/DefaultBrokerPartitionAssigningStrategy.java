package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerPartitionAssigningStrategy.class)
public class DefaultBrokerPartitionAssigningStrategy implements BrokerPartitionAssigningStrategy {

	@Override
	public Map<Integer, Set<String>> assign(List<Partition> partitions, Set<String> brokers,
	      Map<Integer, Set<String>> originAssignment) {
		Map<Integer, Set<String>> result = new HashMap<>();
		int partitionCount = partitions.size();
		int brokerCount = brokers.size();
		List<String> brokerNameList = new ArrayList<>(brokers);

		if (partitionCount == 0 || brokerCount == 0) {
			return result;
		}

		for (Partition partition : partitions) {
			result.put(partition.getId(), new HashSet<String>());
		}

		int brokerPos = 0;
		for (Partition partition : partitions) {
			result.get(partition.getId()).add(brokerNameList.get(brokerPos));
			brokerPos = (brokerPos + 1) % brokerCount;
		}

		return result;
	}
}
