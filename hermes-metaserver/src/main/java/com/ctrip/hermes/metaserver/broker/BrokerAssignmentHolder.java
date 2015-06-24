package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssignmentHolder.class)
public class BrokerAssignmentHolder {

	private static final Logger log = LoggerFactory.getLogger(BrokerAssignmentHolder.class);

	@Inject
	private ZookeeperService m_zkService;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private BrokerPartitionAssigningStrategy m_brokerassigningStrategy;

	private AtomicReference<Map<String, Assignment<Integer>>> m_assignments = new AtomicReference<>();

	private AtomicReference<Map<String, ClientContext>> m_brokersCache = new AtomicReference<>();

	private AtomicReference<List<Topic>> m_topicsCache = new AtomicReference<>();

	public BrokerAssignmentHolder() {
		m_assignments.set(new HashMap<String, Assignment<Integer>>());
	}

	public Assignment<Integer> getAssignment(String topic) {
		return m_assignments.get().get(topic);
	}

	public Map<String, Assignment<Integer>> getAssignments() {
		return m_assignments.get();
	}

	public void reassign(Map<String, ClientContext> brokers) {
		reassign(brokers, null);
	}

	public void reassign(List<Topic> topics) {
		reassign(null, topics);
	}

	public void reassign(Map<String, ClientContext> brokers, List<Topic> topics) {
		if (brokers != null) {
			m_brokersCache.set(brokers);
		}

		if (topics != null) {
			m_topicsCache.set(topics);
		}
		Map<String, Assignment<Integer>> newAssignments = m_brokerassigningStrategy.assign(m_brokersCache.get(),
		      m_topicsCache.get(), getAssignments());
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

	private void setAssignments(Map<String, Assignment<Integer>> newAssignments) {
		if (newAssignments != null) {
			m_assignments.set(newAssignments);

			persistToZk(newAssignments);
		}
	}

	private void persistToZk(Map<String, Assignment<Integer>> assignments) {
		if (assignments != null) {
			for (Map.Entry<String, Assignment<Integer>> entry : assignments.entrySet()) {
				String topic = entry.getKey();
				Assignment<Integer> topicAssignment = entry.getValue();
				for (Map.Entry<Integer, Map<String, ClientContext>> partitionAssignment : topicAssignment.getAssigment()
				      .entrySet()) {
					String path = ZKPathUtils.getBrokerAssignmentZkPath(topic, partitionAssignment.getKey());
					try {
						m_zkService.persist(path, ZKSerializeUtils.serialize(partitionAssignment.getValue()));
					} catch (Exception e) {
						log.error("Failed to persisit broker assignments to zk.", e);
					}
				}
			}
		}

	}

	public void reload() {
		Map<String, Assignment<Integer>> assignments = loadFromZk();
		if (assignments != null) {
			m_assignments.set(assignments);
		}
	}

	private Map<String, Assignment<Integer>> loadFromZk() {
		Map<String, Assignment<Integer>> assignments = new HashMap<String, Assignment<Integer>>();

		try {
			String rootPath = ZKPathUtils.getBrokerAssignmentRootZkPath();
			CuratorFramework client = m_zkClient.getClient();

			m_zkService.ensurePath(rootPath);

			List<String> topics = client.getChildren().forPath(rootPath);

			if (topics != null && !topics.isEmpty()) {
				for (String topic : topics) {
					String topicPath = ZKPathUtils.getBrokerAssignmentTopicParentZkPath(topic);

					List<String> partitions = client.getChildren().forPath(topicPath);
					if (partitions != null && !partitions.isEmpty()) {
						Assignment<Integer> assignment = new Assignment<Integer>();
						assignments.put(topic, assignment);

						for (String partition : partitions) {
							int partitionId = Integer.valueOf(partition);
							String partitionPath = ZKPathUtils.getBrokerAssignmentZkPath(topic, partitionId);
							byte[] data = client.getData().forPath(partitionPath);
							Map<String, ClientContext> clients = ZKSerializeUtils.deserialize(data,
							      new TypeReference<Map<String, ClientContext>>() {
							      }.getType());
							if (clients != null) {
								assignment.addAssignment(partitionId, clients);
							}
						}
					}
				}
			}

		} catch (Exception e) {
			log.error("Failed to load broker assignments from zk.", e);
		}

		return assignments;
	}

}
