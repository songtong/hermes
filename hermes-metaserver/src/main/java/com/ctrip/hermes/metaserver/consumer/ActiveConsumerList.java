package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ActiveConsumerList {
	private boolean m_changed = true;

	private Map<String, Long> m_consumers = new HashMap<>();

	public void heartbeat(String consumerName, long heartbeatTime) {
		if (!m_consumers.containsKey(consumerName)) {
			m_changed = true;
		}
		m_consumers.put(consumerName, heartbeatTime);
	}

	public void purgeExpired(long timeoutMillis, long now) {
		Iterator<Entry<String, Long>> iterator = m_consumers.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Long> entry = iterator.next();
			if (entry.getValue() + timeoutMillis < now) {
				iterator.remove();
				m_changed = true;
			}
		}
	}

	public boolean getAndResetChanged() {
		boolean changed = m_changed;
		m_changed = false;
		return changed;
	}

	public Set<String> getActiveConsumerNames() {
		return m_consumers.keySet();
	}
}
