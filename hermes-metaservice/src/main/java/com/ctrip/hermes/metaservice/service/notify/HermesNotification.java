package com.ctrip.hermes.metaservice.service.notify;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.model.Notification;

public class HermesNotification {
	private NotificationType m_type;

	private List<String> m_receivers;

	private Object m_content;

	public HermesNotification(NotificationType type, List<String> receivers, Object content) {
		m_type = type;
		m_receivers = receivers;
		m_content = content;
	}

	public NotificationType getType() {
		return m_type;
	}

	public List<String> getReceivers() {
		return m_receivers;
	}

	public Object getContent() {
		return m_content;
	}

	public Notification toDBENtity() {
		Notification notification = new Notification();
		notification.setContent(JSON.toJSONString(m_content));
		notification.setReceivers(JSON.toJSONString(m_receivers));
		notification.setNotificationType(m_type.name());
		return notification;
	}

	@Override
	public String toString() {
		return "HermesNotification [m_type=" + m_type + ", m_receivers=" + m_receivers + ", m_content=" + m_content + "]";
	}
}
