package com.ctrip.hermes.metaserver.broker;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerPartitionAssigningStrategy {

	public Map<Integer, Map<String, ClientContext>> assign(List<Partition> partitions,
	      Map<String, ClientContext> brokers, Map<Integer, Map<String, ClientContext>> originAssignment);
}
