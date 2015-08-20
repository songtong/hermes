package com.ctrip.hermes.rest.status;


public class Tge {

	private String m_topic;

	private String m_group;

	private String m_endpoint;

	public Tge(String topic, String group, String endpoint) {
		this.m_topic = topic;
		this.m_group = group;
		this.m_endpoint = endpoint;
	}

	public String getEndpoint() {
		return m_endpoint;
	}

	public String getGroup() {
		return m_group;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setEndpoint(String m_endpoint) {
		this.m_endpoint = m_endpoint;
	}

	public void setGroup(String m_group) {
		this.m_group = m_group;
	}

	public void setTopic(String m_topic) {
		this.m_topic = m_topic;
	}

	public int hashCode() {
		return this.m_topic.hashCode() * 37 * this.m_group.hashCode() * this.m_endpoint.hashCode();
	}

	public boolean equals(Object rhs) {
		if (rhs == null)
			return false;
		if (!(rhs instanceof Tge)) {
			return false;
		}
		Tge rObj = (Tge) rhs;
		return this.m_topic.equals(rObj.m_topic) && this.m_group.equals(rObj.m_group)
		      && this.m_endpoint.equals(rObj.m_endpoint);
	}

	public String toString() {
		return new StringBuilder().append(m_topic).append('-').append(m_group).append('-').append(m_endpoint).toString();
	}
}
