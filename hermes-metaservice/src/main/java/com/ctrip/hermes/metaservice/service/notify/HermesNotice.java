package com.ctrip.hermes.metaservice.service.notify;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.model.Notification;

public class HermesNotice {
	private List<String> m_receivers;

	private NoticeContent m_content;

	public HermesNotice(List<String> receivers, NoticeContent content) {
		m_receivers = receivers;
		m_content = content;
	}

	public NoticeType getType() {
		return m_content.getType();
	}

	public List<String> getReceivers() {
		return m_receivers;
	}

	public NoticeContent getContent() {
		return m_content;
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
