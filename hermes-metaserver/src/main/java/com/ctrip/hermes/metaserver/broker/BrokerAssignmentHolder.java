package com.ctrip.hermes.metaserver.broker;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.ActiveClientListHolder;
import com.ctrip.hermes.metaserver.commons.BaseAssignmentHolder;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssignmentHolder.class)
public class BrokerAssignmentHolder extends BaseAssignmentHolder<String, Integer> {

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private BrokerPartitionAssigningStrategy m_partitionAssigningStrategy;

	@Inject
	private ActiveBrokerListHolder m_activeBrokerListHolder;

	@Override
	protected Assignment createNewAssignment(String topicName, Set<String> brokers,
	      BaseAssignmentHolder<String, Integer>.Assignment originAssignment) {
		Topic topic = m_metaHolder.getMeta().findTopic(topicName);
		if (topic != null) {
			List<Partition> partitions = topic.getPartitions();
			if (partitions == null || partitions.isEmpty()) {
				return null;
			}

			Map<Integer, Set<String>> assigns = m_partitionAssigningStrategy.assign(partitions, brokers,
			      originAssignment == null ? null : originAssignment.getAssigment());

			if (assigns == null) {
				return null;
			}

			Assignment assignment = new Assignment();

			for (Map.Entry<Integer, Set<String>> entry : assigns.entrySet()) {
				assignment.addAssignment(entry.getKey(), entry.getValue());
			}

			return assignment;
		} else {
			return null;
		}
	}

	@Override
	protected long getClientTimeoutMillis() {
		return m_config.getBrokerHeartbeatTimeoutMillis();
	}

	@Override
	protected long getAssignmentCheckIntervalMillis() {
		return m_config.getActiveBrokerCheckIntervalTimeMillis();
	}

	@Override
	protected String getAssignmentCheckerName() {
		return "BrokerRebalanceChecker";
	}

	@Override
	protected ActiveClientListHolder<String> getActiveClientListHolder() {
		return m_activeBrokerListHolder;
	}

}
