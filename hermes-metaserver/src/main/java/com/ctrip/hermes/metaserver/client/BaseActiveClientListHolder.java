package com.ctrip.hermes.metaserver.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BaseActiveClientListHolder<Key> implements ActiveClientListHolder<Key> {
	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private MetaServerConfig m_config;

	private Map<Key, ActiveClientList> m_activeClientLists = new HashMap<>();

	private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();

	@Override
	public void heartbeat(Key key, String clientName) {
		m_lock.writeLock().lock();
		try {
			if (!m_activeClientLists.containsKey(key)) {
				m_activeClientLists.put(key, new ActiveClientList());
			}
			ActiveClientList activeClientList = m_activeClientLists.get(key);
			activeClientList.heartbeat(clientName, m_systemClockService.now());
		} finally {
			m_lock.writeLock().unlock();
		}

	}

	@Override
	public Map<Key, Set<String>> scanChanges(long timeout, TimeUnit timeUnit) {
		Map<Key, Set<String>> changes = new HashMap<>();
		long timeoutMillis = timeUnit.toMillis(timeout);
		m_lock.writeLock().lock();
		try {
			Iterator<Entry<Key, ActiveClientList>> iterator = m_activeClientLists.entrySet().iterator();

			while (iterator.hasNext()) {
				Entry<Key, ActiveClientList> entry = iterator.next();
				ActiveClientList activeClientList = entry.getValue();
				if (activeClientList != null) {
					activeClientList.purgeExpired(timeoutMillis, m_systemClockService.now());

					Set<String> activeClientNames = activeClientList.getActiveClientNames();

					if (activeClientNames == null || activeClientNames.isEmpty()) {
						iterator.remove();
						changes.put(entry.getKey(), activeClientNames);
					} else {
						if (activeClientList.getAndResetChanged()) {
							changes.put(entry.getKey(), activeClientNames);
						}
					}
				} else {
					iterator.remove();
				}
			}
		} finally {
			m_lock.writeLock().unlock();
		}

		return changes;
	}

	@Override
	public ActiveClientList getActiveClientList(Key key) {
		m_lock.readLock().lock();
		try {
			return m_activeClientLists.get(key);
		} finally {
			m_lock.readLock().unlock();
		}
	}

}
