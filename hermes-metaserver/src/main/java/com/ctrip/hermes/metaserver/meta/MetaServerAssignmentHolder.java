package com.ctrip.hermes.metaserver.meta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Server;
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
@Named(type = MetaServerAssignmentHolder.class)
public class MetaServerAssignmentHolder {

	private static final Logger log = LoggerFactory.getLogger(MetaServerAssignmentHolder.class);

	@Inject
	private ZookeeperService m_zkService;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private MetaServerAssigningStrategy m_metaServerAssigningStrategy;

	private AtomicReference<Assignment<String>> m_assignments = new AtomicReference<>();

	private AtomicReference<List<Server>> m_metaServersCache = new AtomicReference<>();

	private AtomicReference<List<Topic>> m_topicsCache = new AtomicReference<>();

	private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();

	public MetaServerAssignmentHolder() {
		m_assignments.set(new Assignment<String>());
	}

	public Map<String, ClientContext> getAssignment(String topic) {
		m_lock.readLock().lock();
		try {
			return m_assignments.get().getAssignment(topic);
		} finally {
			m_lock.readLock().unlock();
		}
	}

	public Assignment<String> getAssignments() {
		m_lock.readLock().lock();
		try {
			return m_assignments.get();
		} finally {
			m_lock.readLock().unlock();
		}
	}

	public void reassign(List<Server> metaServers, List<Topic> topics) {
		m_lock.writeLock().lock();
		try {
			if (metaServers != null) {
				m_metaServersCache.set(metaServers);
			}

			if (topics != null) {
				m_topicsCache.set(topics);
			}
		} finally {
			m_lock.writeLock().unlock();
		}
		Assignment<String> newAssignments = m_metaServerAssigningStrategy.assign(m_metaServersCache.get(),
		      m_topicsCache.get(), getAssignments());
		setAssignments(newAssignments);

		if (log.isDebugEnabled()) {
			log.debug("Meta server assignment changed.(new assignment={})", newAssignments.toString());
		}
	}

	private void setAssignments(Assignment<String> newAssignments) {
		if (newAssignments != null) {
			m_lock.writeLock().lock();
			try {
				m_assignments.set(newAssignments);
			} finally {
				m_lock.writeLock().unlock();
			}

			persistToZk(newAssignments);
		}
	}

	private void persistToZk(Assignment<String> assignments) {
		if (assignments != null) {
			for (Map.Entry<String, Map<String, ClientContext>> entry : assignments.getAssigment().entrySet()) {
				String topic = entry.getKey();
				Map<String, ClientContext> metaServers = entry.getValue();

				String path = ZKPathUtils.getMetaServerAssignmentZkPath(topic);
				try {
					m_zkService.persist(path, ZKSerializeUtils.serialize(metaServers),
					      ZKPathUtils.getMetaServerAssignmentRootZkPath());
				} catch (Exception e) {
					log.error("Failed to persisit meta server assignments to zk.", e);
				}
			}
		}
	}

	public void reload() {
		Assignment<String> assignments = loadFromZk();
		if (assignments != null) {
			m_lock.writeLock().lock();
			try {
				m_assignments.set(assignments);
			} finally {
				m_lock.writeLock().unlock();
			}
		}
	}

	public Assignment<String> loadFromZk() {
		Assignment<String> assignments = new Assignment<String>();

		try {
			String rootPath = ZKPathUtils.getMetaServerAssignmentRootZkPath();
			CuratorFramework client = m_zkClient.getClient();

			m_zkService.ensurePath(rootPath);

			List<String> topics = client.getChildren().forPath(rootPath);

			if (topics != null && !topics.isEmpty()) {
				for (String topic : topics) {
					String topicPath = ZKPathUtils.getMetaServerAssignmentZkPath(topic);

					byte[] data = client.getData().forPath(topicPath);

					Map<String, ClientContext> clients = ZKSerializeUtils.deserialize(data,
					      new TypeReference<Map<String, ClientContext>>() {
					      }.getType());

					if (clients != null) {
						assignments.addAssignment(topic, clients);
					}
				}
			}

		} catch (Exception e) {
			log.error("Failed to load broker assignments from zk.", e);
		}

		return assignments;
	}
}
