package com.ctrip.hermes.metaserver.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = OrderedConsumeConsumerPartitionAssigningStrategy.class)
public class DefaultOrderedConsumeConsumerPartitionAssigningStrategy implements OrderedConsumeConsumerPartitionAssigningStrategy {

	@Override
	public Map<String, List<Integer>> assign(List<Partition> partitions, Set<String> consumers) {
		Map<String, List<Integer>> result = new HashMap<>();
		int partitionCount = partitions.size();
		int consumerCount = consumers.size();
		List<String> consumerNameList = new ArrayList<>(consumers);

		if (partitionCount == 0 || consumerCount == 0) {
			return result;
		}

		for (String consumer : consumers) {
			result.put(consumer, new LinkedList<Integer>());
		}

		int consumerPos = 0;
		for (int i = 0; i < partitionCount; i++) {
			String consumerName = consumerNameList.get(consumerPos);
			result.get(consumerName).add(partitions.get(i).getId());
			consumerPos = (consumerPos + 1) % consumerCount;
		}

		return result;
	}
}
