package com.ctrip.hermes.rest.status;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

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
		return HashCodeBuilder.reflectionHashCode(this);
	}

	public boolean equals(Object rhs) {
		return EqualsBuilder.reflectionEquals(this, rhs);
	}

	public String toString() {
		return new StringBuilder().append(m_topic).append('-').append(m_group).append('-').append(m_endpoint).toString();
	}
}
