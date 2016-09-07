package com.ctrip.hermes.metaservice.service.notify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.model.Notification;

public class HermesNotice {
	private List<String> m_receivers;

	private HermesNoticeContent m_content;

	public HermesNotice(String receiver, HermesNoticeContent content) {
		this(Arrays.asList(receiver), content);
	}

	public HermesNotice(List<String> receivers, HermesNoticeContent content) {
		m_receivers = receivers instanceof ArrayList ? receivers : new ArrayList<String>(receivers);
		m_content = content;
	}

	public HermesNoticeType getType() {
		return m_content.getType();
	}

	public List<String> getReceivers() {
		return m_receivers;
	}

	public HermesNoticeContent getContent() {
		return m_content;
	}

	public void setReceivers(List<String> receivers) {
		m_receivers = receivers;
	}

	public void setContent(HermesNoticeContent content) {
		m_content = content;
	}

	public Notification toNotification() {
		Notification notification = new Notification();
		notification.setContent(JSON.toJSONString(m_content));
		notification.setReceivers(JSON.toJSONString(m_receivers));
		notification.setNotificationType(getType().name());
		return notification;
	}

	@Override
	public String toString() {
		return "HermesNotice [m_receivers=" + m_receivers + ", m_content=" + m_content + "]";
	}
}
