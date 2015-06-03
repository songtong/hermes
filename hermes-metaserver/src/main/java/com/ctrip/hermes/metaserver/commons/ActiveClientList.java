package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ActiveClientList {
	private boolean m_changed = true;

	private Map<String, ClientContext> m_clients = new HashMap<>();

	public void heartbeat(String clientName, long heartbeatTime, String ip, int port) {
		if (!m_clients.containsKey(clientName)) {
			m_changed = true;
			m_clients.put(clientName, new ClientContext(clientName, ip, port, heartbeatTime));
		} else {
			m_clients.get(clientName).setLastHeartbeatTime(heartbeatTime);
		}
	}

	public void purgeExpired(long timeoutMillis, long now) {
		Iterator<Entry<String, ClientContext>> iterator = m_clients.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, ClientContext> entry = iterator.next();
			if (entry.getValue().getLastHeartbeatTime() + timeoutMillis < now) {
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

	public Map<String, ClientContext> getActiveClients() {
		return new HashMap<>(m_clients);
	}

	public static class ClientContext {
		private String m_name;

		private String m_ip;

		private int m_port;

		private long lastHeartbeatTime;

		public ClientContext(String name, String ip, int port, long lastHeartbeatTime) {
			m_name = name;
			m_ip = ip;
			m_port = port;
			this.lastHeartbeatTime = lastHeartbeatTime;
		}

		public String getName() {
			return m_name;
		}

		public void setName(String name) {
			m_name = name;
		}

		public String getIp() {
			return m_ip;
		}

		public void setIp(String ip) {
			m_ip = ip;
		}

		public int getPort() {
			return m_port;
		}

		public void setPort(int port) {
			m_port = port;
		}

		public long getLastHeartbeatTime() {
			return lastHeartbeatTime;
		}

		public void setLastHeartbeatTime(long lastHeartbeatTime) {
			this.lastHeartbeatTime = lastHeartbeatTime;
		}

		@Override
		public String toString() {
			return "ClientContext [m_name=" + m_name + ", m_ip=" + m_ip + ", m_port=" + m_port + ", lastHeartbeatTime="
			      + lastHeartbeatTime + "]";
		}

	}
}
