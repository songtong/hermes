package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerPartitionAssigningStrategy.class)
public class DefaultBrokerPartitionAssigningStrategy implements BrokerPartitionAssigningStrategy {

	@Override
	public Map<String, Assignment<Integer>> assign(Map<String, ClientContext> brokers, List<Topic> topics,
	      Map<String, Assignment<Integer>> originAssignment) {
		Map<String, Assignment<Integer>> newAssignments = new HashMap<>();
		if (brokers != null && !brokers.isEmpty()) {
			if (topics != null && !topics.isEmpty()) {
				List<Entry<String, ClientContext>> brokerEntries = new ArrayList<>(brokers.entrySet());

				int brokerPos = 0;
				int brokerCount = brokers.size();
				for (Topic topic : topics) {
					if (Endpoint.BROKER.equals(topic.getEndpointType())) {
						List<Partition> partitions = topic.getPartitions();
						if (partitions != null && !partitions.isEmpty()) {

							Assignment<Integer> assignment = new Assignment<>();
							newAssignments.put(topic.getName(), assignment);

							for (Partition partition : partitions) {
								Entry<String, ClientContext> brokerEntry = brokerEntries.get(brokerPos);
								brokerPos = (brokerPos + 1) % brokerCount;
								Map<String, ClientContext> broker = new HashMap<>();
								broker.put(brokerEntry.getKey(), brokerEntry.getValue());
								assignment.addAssignment(partition.getId(), broker);
							}

						}
					}
				}
			}
		}

		return newAssignments;
	}
}
