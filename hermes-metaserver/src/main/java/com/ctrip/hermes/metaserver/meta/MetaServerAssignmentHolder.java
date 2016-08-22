package com.ctrip.hermes.metaserver.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.log.LoggerConstants;
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

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

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

	public void reassign(List<Server> metaServers, Map<Pair<String, Integer>, Server> configedMetaServers,
	      List<Topic> topics) {
		m_lock.writeLock().lock();
		try {
			if (metaServers != null) {
				setMetaServersCache(metaServers, configedMetaServers);
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

		if (traceLog.isInfoEnabled()) {
			traceLog.info("Meta server assignment changed.(new assignment={})", newAssignments.toString());
		}
	}

	private void setMetaServersCache(List<Server> metaServers, Map<Pair<String, Integer>, Server> configedMetaServers) {
		if (metaServers != null && configedMetaServers != null) {
			List<Server> mergedMetaServers = new ArrayList<>(metaServers.size());
			for (Server metaServer : metaServers) {
				Server configedMetaServer = configedMetaServers.get(new Pair<String, Integer>(metaServer.getHost(),
				      metaServer.getPort()));
				if (configedMetaServer != null) {
					Server tmpServer = new Server();
					tmpServer.setEnabled(configedMetaServer.getEnabled());
					tmpServer.setHost(configedMetaServer.getHost());
					tmpServer.setId(metaServer.getId());
					tmpServer.setIdc(configedMetaServer.getIdc());
					tmpServer.setPort(configedMetaServer.getPort());
					mergedMetaServers.add(tmpServer);
				}
			}

			m_metaServersCache.set(mergedMetaServers);
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
			Map<String, byte[]> zkPathAndDatas = new HashMap<>();
			for (Map.Entry<String, Map<String, ClientContext>> entry : assignments.getAssignments().entrySet()) {
				String topic = entry.getKey();
				Map<String, ClientContext> metaServers = entry.getValue();

				String path = ZKPathUtils.getMetaServerAssignmentZkPath(topic);
				byte[] data = ZKSerializeUtils.serialize(metaServers);

				zkPathAndDatas.put(path, data);
			}

			try {
				m_zkService.persistBulk(zkPathAndDatas, ZKPathUtils.getMetaServerAssignmentRootZkPath());
			} catch (Exception e) {
				log.error("Failed to persisit meta server assignments to zk.", e);
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
			CuratorFramework client = m_zkClient.get();

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
