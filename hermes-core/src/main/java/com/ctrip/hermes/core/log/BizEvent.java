package com.ctrip.hermes.core.log;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class BizEvent {

	private String m_eventType;

	private Date m_eventTime;

	private Map<String, Object> m_datas = new HashMap<String, Object>();

	public BizEvent(String eventType) {
		m_eventType = eventType;
		m_eventTime = new Date();
	}

	public BizEvent(String eventType, Date eventTime) {
		m_eventType = eventType;
		m_eventTime = eventTime;
	}

	public BizEvent addData(String key, Object value) {
		m_datas.put(key, value);

		return this;
	}

	public String getEventType() {
		return m_eventType;
	}

	public Date getEventTime() {
		return m_eventTime;
	}

	public Map<String, Object> getDatas() {
		return m_datas;
	}

}
