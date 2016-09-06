package com.ctrip.hermes.metaserver.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaServerAssigningStrategy.class)
public class LeastAdjustmentMetaServerAssigningStrategy implements MetaServerAssigningStrategy {

	private final static Logger log = LoggerFactory.getLogger(LeastAdjustmentMetaServerAssigningStrategy.class);

	@Override
	public Assignment<String> assign(List<Server> metaServers, List<Topic> topics, Assignment<String> originAssignments) {
		Assignment<String> newAssignments = new Assignment<>();

		if (metaServers == null || metaServers.isEmpty() || topics == null || topics.isEmpty()) {
			return newAssignments;
		}

		if (originAssignments == null) {
			originAssignments = new Assignment<>();
		}
		originAssignments = removeDeletedTopics(topics, originAssignments);

		Map<String, List<String>> originMetaServerToTopic = mapMetaServerToTopics(originAssignments);

		Set<String> originMetaServers = originMetaServerToTopic.keySet();

		Map<String, Server> currentMetaServers = new HashMap<>();
		for (Server server : metaServers) {
			currentMetaServers.put(server.getId(), server);
		}

		List<String> deletedMetaServers = setMinus(originMetaServers, currentMetaServers.keySet());
		List<String> addedMetaServers = setMinus(currentMetaServers.keySet(), originMetaServers);
		List<String> commonMetaServers = setIntersect(originMetaServers, currentMetaServers.keySet());

		List<String> neverAssignedTopicNames = findNeverAssignedTopics(topics, originAssignments.getAssignments()
		      .keySet());
		List<String> assignLostTopics = findTopics(deletedMetaServers, originMetaServerToTopic);

		List<String> freeTopics = new LinkedList<>();
		freeTopics.addAll(neverAssignedTopicNames);
		freeTopics.addAll(assignLostTopics);

		AssignBalancer<String> allocator = new AssignBalancer<>(topics.size(), Math.min(currentMetaServers.size(),
		      topics.size()), freeTopics);

		for (String commonMetaServer : commonMetaServers) {
			List<String> originAssign = originMetaServerToTopic.get(commonMetaServer);
			List<String> newAssign = allocator.adjust(originAssign);
			putAssignToResult(newAssignments, currentMetaServers, commonMetaServer, newAssign);
		}

		for (String addedMetaServer : addedMetaServers) {
			List<String> newAssign = allocator.adjust(Collections.<String> emptyList());
			putAssignToResult(newAssignments, currentMetaServers, addedMetaServer, newAssign);
		}

		return newAssignments;
	}

	private Assignment<String> removeDeletedTopics(List<Topic> topics, Assignment<String> originAssignments) {
		Set<String> topicNames = new HashSet<>();
		for (Topic topic : topics) {
			topicNames.add(topic.getName());
		}

		Assignment<String> assignment = new Assignment<>();

		for (Map.Entry<String, Map<String, ClientContext>> entry : originAssignments.getAssignments().entrySet()) {
			String topicName = entry.getKey();
			if (topicNames.contains(topicName)) {
				assignment.addAssignment(topicName, entry.getValue());
			}
		}

		return assignment;
	}

	private void putAssignToResult(Assignment<String> newAssignments, Map<String, Server> currentMetaServers,
	      String metaServerName, List<String> newAssign) {
		for (String topic : newAssign) {
			Map<String, ClientContext> server = new HashMap<>();
			Server metaServer = currentMetaServers.get(metaServerName);
			server.put(metaServerName, new ClientContext(metaServer.getId(), metaServer.getHost(), metaServer.getPort(),
			      null, metaServer.getIdc(), -1));
			newAssignments.addAssignment(topic, server);
		}
	}

	private List<String> findNeverAssignedTopics(List<Topic> topics, Set<String> originAssign) {
		List<String> result = new ArrayList<>();

		for (Topic topic : topics) {
			if (!originAssign.contains(topic.getName())) {
				result.add(topic.getName());
			}
		}

		return result;
	}

	private List<String> findTopics(List<String> metaServers, Map<String, List<String>> metaServerToTopic) {
		List<String> result = new ArrayList<>();

		for (String metaServer : metaServers) {
			List<String> topics = metaServerToTopic.get(metaServer);
			if (topics != null) {
				result.addAll(topics);
			}
		}

		return result;
	}

	private List<String> setIntersect(Set<String> left, Set<String> right) {
		List<String> result = new ArrayList<>(left);
		result.retainAll(right);

		Collections.sort(result);
		return result;
	}

	private List<String> setMinus(Set<String> left, Set<String> right) {
		List<String> result = new ArrayList<>(left);
		result.removeAll(right);

		Collections.sort(result);
		return result;
	}

	private Map<String, List<String>> mapMetaServerToTopics(Assignment<String> originAssignments) {
		Map<String, List<String>> result = new HashMap<>();
		for (Map.Entry<String, Map<String, ClientContext>> entry : originAssignments.getAssignments().entrySet()) {
			String topic = entry.getKey();
			if (entry.getValue().size() != 1) {
				log.warn("Topic {} have more than one metaServer assigned", topic);
			}

			String metaServer = entry.getValue().keySet().iterator().next();

			List<String> topics = result.get(metaServer);
			if (topics == null) {
				topics = new ArrayList<>();
				result.put(metaServer, topics);
			}
			topics.add(topic);
		}

		for (Map.Entry<String, List<String>> entry : result.entrySet()) {
			Collections.sort(entry.getValue());
		}

		return result;
	}
}
