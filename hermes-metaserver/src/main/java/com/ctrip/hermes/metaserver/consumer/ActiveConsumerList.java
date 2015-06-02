package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.Map;
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
		for (String consumerName : m_consumers.keySet()) {
			Long lastHeartbeatTime = m_consumers.get(consumerName);
			if (lastHeartbeatTime + timeoutMillis < now) {
				m_consumers.remove(consumerName);
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
