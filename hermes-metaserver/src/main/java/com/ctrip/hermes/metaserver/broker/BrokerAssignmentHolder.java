package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssignmentHolder.class)
public class BrokerAssignmentHolder {

	private static final Logger log = LoggerFactory.getLogger(BrokerAssignmentHolder.class);

	@Inject
	private BrokerPartitionAssigningStrategy m_brokerAssigningStrategy;

	private AtomicReference<Map<String, Assignment<Integer>>> m_assignments = new AtomicReference<>();

	private AtomicReference<Map<String, List<Topic>>> m_topicsCache = new AtomicReference<>();

	private AtomicReference<Map<String, Map<String, ClientContext>>> m_mergedBrokers;

	private AtomicReference<Map<Pair<String, Integer>, Endpoint>> m_configedBrokers;

	private AtomicReference<Map<String, ClientContext>> m_runningBrokers;

	public BrokerAssignmentHolder() {
		m_assignments.set(new HashMap<String, Assignment<Integer>>());

		m_mergedBrokers = new AtomicReference<>();
		m_mergedBrokers.set(new HashMap<String, Map<String, ClientContext>>());

		m_configedBrokers = new AtomicReference<>();
		m_configedBrokers.set(new HashMap<Pair<String, Integer>, Endpoint>());

		m_runningBrokers = new AtomicReference<>();
		m_runningBrokers.set(new HashMap<String, ClientContext>());
	}

	private Map<String, ClientContext> getBrokers(String brokerGroup) {
		Map<String, Map<String, ClientContext>> allBrokers = m_mergedBrokers.get();
		if (allBrokers != null) {
			return allBrokers.get(brokerGroup);
		}
		return null;
	}

	private void setConfigedBrokers(List<Endpoint> endpoints) {
		if (endpoints != null) {
			Map<Pair<String, Integer>, Endpoint> configedBrokers = new HashMap<Pair<String, Integer>, Endpoint>(
			      endpoints.size());
			for (Endpoint endpoint : endpoints) {
				if (Endpoint.BROKER.equals(endpoint.getType())) {
					configedBrokers.put(new Pair<String, Integer>(endpoint.getHost(), endpoint.getPort()), endpoint);
				}
			}

			m_configedBrokers.set(configedBrokers);
		}
	}

	private void setRunningBrokers(Map<String, ClientContext> runningBrokers) {
		if (runningBrokers != null) {
			m_runningBrokers.set(new HashMap<String, ClientContext>(runningBrokers));
		}
	}

	private void mergeAvailableBrokers() {
		Map<String, Map<String, ClientContext>> aggregateBrokers = new HashMap<String, Map<String, ClientContext>>();
		Map<String, ClientContext> runningBrokers = m_runningBrokers.get();
		Map<Pair<String, Integer>, Endpoint> configedBrokers = m_configedBrokers.get();

		if (runningBrokers != null && !runningBrokers.isEmpty() && configedBrokers != null && !configedBrokers.isEmpty()) {
			for (Map.Entry<String, ClientContext> entry : runningBrokers.entrySet()) {
				ClientContext currentBroker = entry.getValue();
				if (currentBroker != null) {
					Pair<String, Integer> ipPort = new Pair<>(currentBroker.getIp(), currentBroker.getPort());
					Endpoint configedBroker = configedBrokers.get(ipPort);
					if (configedBroker != null) {
						String brokerGroup = configedBroker.getGroup();
						if (!aggregateBrokers.containsKey(brokerGroup)) {
							aggregateBrokers.put(brokerGroup, new HashMap<String, ClientContext>());
						}

						ClientContext existedBroker = aggregateBrokers.get(brokerGroup).get(currentBroker.getName());
						if (existedBroker == null
						      || existedBroker.getLastHeartbeatTime() < currentBroker.getLastHeartbeatTime()) {
							currentBroker.setGroup(brokerGroup);
							aggregateBrokers.get(brokerGroup).put(currentBroker.getName(), currentBroker);
						}
					}
				}
			}
		}

		m_mergedBrokers.set(aggregateBrokers);
	}

	public Assignment<Integer> getAssignment(String topic) {
		return m_assignments.get().get(topic);
	}

	public Map<String, Assignment<Integer>> getAssignments() {
		return m_assignments.get();
	}

	public void reassign(Map<String, ClientContext> runningBrokers) {
		reassign(runningBrokers, null, null);
	}

	public void reassign(List<Endpoint> configedBrokers, List<Topic> topics) {
		reassign(null, configedBrokers, topics);
	}

	public void reassign(List<Topic> topics) {
		reassign(null, null, topics);
	}

	public void reassign(Map<String, ClientContext> runningBrokers, List<Endpoint> configedBrokers, List<Topic> topics) {
		if (runningBrokers != null) {
			setRunningBrokers(runningBrokers);
		}

		if (configedBrokers != null) {
			setConfigedBrokers(configedBrokers);
		}

		mergeAvailableBrokers();

		if (topics != null) {
			m_topicsCache.set(groupTopics(topics));
		}

		Map<String, Assignment<Integer>> newAssignments = assign();
		setAssignments(newAssignments);

		if (log.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();

			sb.append("[");
			for (Map.Entry<String, Assignment<Integer>> entry : newAssignments.entrySet()) {
				sb.append("Topic=").append(entry.getKey()).append(",");
				sb.append("assignment=").append(entry.getValue());
			}
			sb.append("]");

			log.debug("Broker assignment changed.(new assignment={})", sb.toString());
		}
	}

	private Map<String, Assignment<Integer>> assign() {
		Map<String, Assignment<Integer>> newAssignments = new HashMap<>();
		Map<String, List<Topic>> topics = m_topicsCache.get();
		if (topics != null) {
			for (Map.Entry<String, List<Topic>> entry : topics.entrySet()) {
				String brokerGroup = entry.getKey();
				List<Topic> groupTopics = entry.getValue();
				Map<String, ClientContext> groupBrokers = getBrokers(brokerGroup);

				if (groupBrokers != null && !groupBrokers.isEmpty() && groupTopics != null && !groupTopics.isEmpty()) {
					Map<String, Assignment<Integer>> groupAssignments = m_brokerAssigningStrategy.assign(groupBrokers,
					      groupTopics, getAssignments());
					newAssignments.putAll(groupAssignments);
				}
			}
		}
		return newAssignments;
	}

	private Map<String, List<Topic>> groupTopics(List<Topic> topics) {
		Map<String, List<Topic>> topicGroups = new HashMap<>();

		for (Topic topic : topics) {
			if (!StringUtils.isBlank(topic.getBrokerGroup())) {
				if (!topicGroups.containsKey(topic.getBrokerGroup())) {
					topicGroups.put(topic.getBrokerGroup(), new ArrayList<Topic>());
				}

				topicGroups.get(topic.getBrokerGroup()).add(topic);
			} else {
				log.warn("Topic({}) does not have broker group, will ignore it.", topic.getName());
			}
		}

		return topicGroups;
	}

	private void setAssignments(Map<String, Assignment<Integer>> newAssignments) {
		if (newAssignments != null) {
			m_assignments.set(newAssignments);
		}
	}

	public void clear() {
		setAssignments(new HashMap<String, Assignment<Integer>>());
		m_topicsCache.set(new HashMap<String, List<Topic>>());
		m_mergedBrokers.set(new HashMap<String, Map<String, ClientContext>>());
		m_configedBrokers.set(new HashMap<Pair<String, Integer>, Endpoint>());
		m_runningBrokers.set(new HashMap<String, ClientContext>());
	}

}
