package com.ctrip.hermes.metaserver.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ActiveClientList {
	private boolean m_changed = true;

	private Map<String, Long> m_clients = new HashMap<>();

	public void heartbeat(String clientName, long heartbeatTime) {
		if (!m_clients.containsKey(clientName)) {
			m_changed = true;
		}
		m_clients.put(clientName, heartbeatTime);
	}

	public void purgeExpired(long timeoutMillis, long now) {
		Iterator<Entry<String, Long>> iterator = m_clients.entrySet().iterator();
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

	public Set<String> getActiveClientNames() {
		return m_clients.keySet();
	}
}
