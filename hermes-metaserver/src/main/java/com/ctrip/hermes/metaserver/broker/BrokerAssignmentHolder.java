package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

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

	private AtomicReference<Map<String, ClientContext>> m_brokersCache = new AtomicReference<>();

	private AtomicReference<List<Topic>> m_topicsCache = new AtomicReference<>();

	private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();

	public BrokerAssignmentHolder() {
		m_assignments.set(new HashMap<String, Assignment<Integer>>());
	}

	public Assignment<Integer> getAssignment(String topic) {
		m_lock.readLock().lock();
		try {
			return m_assignments.get().get(topic);
		} finally {
			m_lock.readLock().unlock();
		}
	}

	public Map<String, Assignment<Integer>> getAssignments() {
		m_lock.readLock().lock();
		try {
			return m_assignments.get();
		} finally {
			m_lock.readLock().unlock();
		}
	}

	public void reassign(Map<String, ClientContext> brokers) {
		reassign(brokers, null);
	}

	public void reassign(List<Topic> topics) {
		reassign(null, topics);
	}

	public void reassign(Map<String, ClientContext> brokers, List<Topic> topics) {
		m_lock.writeLock().lock();
		try {
			if (brokers != null) {
				m_brokersCache.set(brokers);
			}

			if (topics != null) {
				m_topicsCache.set(topics);
			}
		} finally {
			m_lock.writeLock().unlock();
		}
		Map<String, Assignment<Integer>> newAssignments = m_brokerAssigningStrategy.assign(m_brokersCache.get(),
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
			m_lock.writeLock().lock();
			try {
				m_assignments.set(newAssignments);
			} finally {
				m_lock.writeLock().unlock();
			}
		}
	}

	public void clear() {
		setAssignments(new HashMap<String, Assignment<Integer>>());
	}

}
