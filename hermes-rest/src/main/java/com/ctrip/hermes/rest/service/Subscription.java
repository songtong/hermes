package com.ctrip.hermes.rest.service;

import java.util.List;

public class Subscription {

	private String m_topic;

	private String m_groupId;

	private List<String> m_pushHttpUrls;

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public void setPushHttpUrls(List<String> pushHttpUrls) {
		m_pushHttpUrls = pushHttpUrls;
	}

	public String getTopic() {
		return m_topic;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public List<String> getPushHttpUrls() {
		return m_pushHttpUrls;
	}

}
