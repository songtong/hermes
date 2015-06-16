package com.ctrip.hermes.core.bo;

public class HostPort {

	private String m_host;

	private int m_port;

	public HostPort() {
	}

	public HostPort(String host, int port) {
		m_host = host;
		m_port = port;
	}

	public String getHost() {
		return m_host;
	}

	public void setHost(String host) {
		m_host = host;
	}

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		m_port = port;
	}

}
