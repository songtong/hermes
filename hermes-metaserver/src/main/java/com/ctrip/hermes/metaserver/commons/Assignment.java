package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Assignment<Key> {
	private Map<Key, Map<String, ClientContext>> m_assigment = new ConcurrentHashMap<>();

	public boolean isAssignTo(Key key, String client) {
		Map<String, ClientContext> clients = m_assigment.get(key);
		return clients != null && !clients.isEmpty() && clients.keySet().contains(client);
	}

	public Map<String, ClientContext> getAssignment(Key key) {
		return m_assigment.get(key);
	}

	public void addAssignment(Key key, Map<String, ClientContext> clients) {
		if (!m_assigment.containsKey(key)) {
			m_assigment.put(key, new HashMap<String, ClientContext>());
		}
		m_assigment.get(key).putAll(clients);
	}

	public Map<Key, Map<String, ClientContext>> getAssigment() {
		return m_assigment;
	}

	@Override
	public String toString() {
		return "Assignment [m_assigment=" + m_assigment + "]";
	}

}
