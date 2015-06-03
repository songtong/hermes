package com.ctrip.hermes.metaserver.broker;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerPartitionAssigningStrategy {

	public Map<Integer, Set<String>> assign(List<Partition> partitions, Set<String> brokers,
	      Map<Integer, Set<String>> originAssignment);
}
