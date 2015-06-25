package com.ctrip.hermes.metaserver.commons;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ClientContext {
	private String m_name;

	private String m_ip;

	private int m_port;

	private long m_lastHeartbeatTime;

	public ClientContext() {
	}

	public ClientContext(String name, String ip, int port, long lastHeartbeatTime) {
		m_name = name;
		m_ip = ip;
		m_port = port;
		m_lastHeartbeatTime = lastHeartbeatTime;
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
		return m_lastHeartbeatTime;
	}

	public void setLastHeartbeatTime(long lastHeartbeatTime) {
		m_lastHeartbeatTime = lastHeartbeatTime;
	}

	@Override
	public String toString() {
		return "ClientContext [m_name=" + m_name + ", m_ip=" + m_ip + ", m_port=" + m_port + ", m_lastHeartbeatTime="
		      + m_lastHeartbeatTime + "]";
	}

}
