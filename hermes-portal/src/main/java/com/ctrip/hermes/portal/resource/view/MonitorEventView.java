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

	private boolean m_shouldNotify;

	public MonitorEventView(MonitorEvent event) {
		m_eventType = MonitorEventType.findByTypeCode(event.getEventType());
		m_key1 = event.getKey1();
		m_key2 = event.getKey2();
		m_key3 = event.getKey3();
		m_key4 = event.getKey4();
		m_message = event.getMessage();
		m_createTime = event.getCreateTime();
		m_notifyTime = event.getNotifyTime();
		m_shouldNotify = event.isShouldNotify();
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

	public boolean isShouldNotify() {
		return m_shouldNotify;
	}

	public void setShouldNotify(boolean shouldNotify) {
		m_shouldNotify = shouldNotify;
	}
}
