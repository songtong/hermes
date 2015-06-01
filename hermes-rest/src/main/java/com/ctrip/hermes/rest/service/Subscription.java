package com.ctrip.hermes.rest.service;

import java.util.List;
import java.util.Objects;

public class Subscription {

	private String m_topic;

	private String m_groupId;

	private List<String> m_endpoints;

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public void setEndpoints(List<String> endpoints) {
		m_endpoints = endpoints;
	}

	public String getTopic() {
		return m_topic;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public List<String> getEndpoints() {
		return m_endpoints;
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.m_topic, this.m_groupId, this.m_endpoints);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Subscription other = (Subscription) obj;
		return Objects.equals(this.m_topic, other.m_topic) && Objects.equals(this.m_groupId, other.m_groupId)
		      && Objects.equals(this.m_endpoints, other.m_endpoints);
	}
}
