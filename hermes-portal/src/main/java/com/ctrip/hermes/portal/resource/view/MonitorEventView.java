package com.ctrip.hermes.portal.resource.view;

import java.util.Date;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class MonitorEventView {
	private MonitorEventType m_eventType;

	private Date m_createTime;

	private String m_key1;

	private String m_key2;

	private String m_key3;

	private String m_key4;

	private String m_message;

	private Date m_notifyTime;

	public MonitorEventView(MonitorEvent event) {
		m_eventType = MonitorEventType.findByTypeCode(event.getEventType());
		m_key1 = event.getKey1();
		m_key2 = event.getKey2();
		m_key3 = event.getKey3();
		m_key4 = event.getKey4();
		m_message = event.getMessage();
		m_createTime = event.getCreateTime();
		m_notifyTime = event.getNotifyTime();
	}

	public MonitorEventType getEventType() {
		return m_eventType;
	}

	public void setEventType(MonitorEventType eventType) {
		m_eventType = eventType;
	}

	public Date getCreateTime() {
		return m_createTime;
	}

	public void setCreateTime(Date createTime) {
		m_createTime = createTime;
	}

	public String getKey1() {
		return m_key1;
	}

	public void setKey1(String key1) {
		m_key1 = key1;
	}

	public String getKey2() {
		return m_key2;
	}

	public void setKey2(String key2) {
		m_key2 = key2;
	}

	public String getKey3() {
		return m_key3;
	}

	public void setKey3(String key3) {
		m_key3 = key3;
	}

	public String getKey4() {
		return m_key4;
	}

	public void setKey4(String key4) {
		m_key4 = key4;
	}

	public String getMessage() {
		return m_message;
	}

	public void setMessage(String message) {
		m_message = message;
	}

	public Date getNotifyTime() {
		return m_notifyTime;
	}

	public void setNotifyTime(Date notifyTime) {
		m_notifyTime = notifyTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_createTime == null) ? 0 : m_createTime.hashCode());
		result = prime * result + ((m_eventType == null) ? 0 : m_eventType.hashCode());
		result = prime * result + ((m_key1 == null) ? 0 : m_key1.hashCode());
		result = prime * result + ((m_key2 == null) ? 0 : m_key2.hashCode());
		result = prime * result + ((m_key3 == null) ? 0 : m_key3.hashCode());
		result = prime * result + ((m_key4 == null) ? 0 : m_key4.hashCode());
		result = prime * result + ((m_message == null) ? 0 : m_message.hashCode());
		result = prime * result + ((m_notifyTime == null) ? 0 : m_notifyTime.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MonitorEventView other = (MonitorEventView) obj;
		if (m_createTime == null) {
			if (other.m_createTime != null)
				return false;
		} else if (!m_createTime.equals(other.m_createTime))
			return false;
		if (m_eventType != other.m_eventType)
			return false;
		if (m_key1 == null) {
			if (other.m_key1 != null)
				return false;
		} else if (!m_key1.equals(other.m_key1))
			return false;
		if (m_key2 == null) {
			if (other.m_key2 != null)
				return false;
		} else if (!m_key2.equals(other.m_key2))
			return false;
		if (m_key3 == null) {
			if (other.m_key3 != null)
				return false;
		} else if (!m_key3.equals(other.m_key3))
			return false;
		if (m_key4 == null) {
			if (other.m_key4 != null)
				return false;
		} else if (!m_key4.equals(other.m_key4))
			return false;
		if (m_message == null) {
			if (other.m_message != null)
				return false;
		} else if (!m_message.equals(other.m_message))
			return false;
		if (m_notifyTime == null) {
			if (other.m_notifyTime != null)
				return false;
		} else if (!m_notifyTime.equals(other.m_notifyTime))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MonitorEventView [m_eventType=" + m_eventType + ", m_createTime=" + m_createTime + ", m_key1=" + m_key1
		      + ", m_key2=" + m_key2 + ", m_key3=" + m_key3 + ", m_key4=" + m_key4 + ", m_message=" + m_message
		      + ", m_notifyTime=" + m_notifyTime + "]";
	}
}
