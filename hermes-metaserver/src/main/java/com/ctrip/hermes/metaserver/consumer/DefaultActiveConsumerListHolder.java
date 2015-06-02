package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ActiveConsumerListHolder.class)
public class DefaultActiveConsumerListHolder implements ActiveConsumerListHolder {
	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private MetaServerConfig m_config;

	private Map<Pair<String, String>, ActiveConsumerList> m_activeConsumers = new HashMap<>();

	private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();

	@Override
	public void heartbeat(String topicName, String consumerGroupName, String consumerName) {
		Pair<String, String> key = new Pair<String, String>(topicName, consumerGroupName);
		m_lock.writeLock().lock();
		try {
			if (!m_activeConsumers.containsKey(key)) {
				m_activeConsumers.put(key, new ActiveConsumerList());
			}
			ActiveConsumerList topicConsumerList = m_activeConsumers.get(key);
			topicConsumerList.heartbeat(consumerName, m_systemClockService.now());
		} finally {
			m_lock.writeLock().unlock();
		}

	}

	@Override
	public Map<Pair<String, String>, Set<String>> scanChanges(long timeout, TimeUnit timeUnit) {
		Map<Pair<String, String>, Set<String>> changedTopicGroupList = new HashMap<>();
		long timeoutMillis = timeUnit.toMillis(timeout);
		m_lock.writeLock().lock();
		try {
			for (Pair<String, String> topicGroup : m_activeConsumers.keySet()) {
				ActiveConsumerList activeConsumerList = m_activeConsumers.get(topicGroup);
				if (activeConsumerList != null) {
					activeConsumerList.purgeExpired(timeoutMillis, m_systemClockService.now());

					Set<String> activeConsumerNames = activeConsumerList.getActiveConsumerNames();

					if (activeConsumerNames == null || activeConsumerNames.isEmpty()) {
						m_activeConsumers.remove(topicGroup);
						changedTopicGroupList.put(topicGroup, activeConsumerNames);
					} else {
						if (activeConsumerList.getAndResetChanged()) {
							changedTopicGroupList.put(topicGroup, activeConsumerNames);
						}
					}
				} else {
					m_activeConsumers.remove(topicGroup);
				}
			}
		} finally {
			m_lock.writeLock().unlock();
		}

		return changedTopicGroupList;
	}

	@Override
	public ActiveConsumerList getActiveConsumerList(String topicName, String consumerGroupName) {
		m_lock.readLock().lock();
		try {
			return m_activeConsumers.get(new Pair<String, String>(topicName, consumerGroupName));
		} finally {
			m_lock.readLock().unlock();
		}
	}

}
