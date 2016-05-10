package com.ctrip.hermes.collector.rule;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * @author tenglinxiao
 *
 */
public class RuleEvent implements Event {
	private String m_eventSource;
	private Object m_data;
	
	public RuleEvent() {}
	
	public RuleEvent(Object data, String eventSource) {
		this.m_data = data;
		this.m_eventSource = eventSource;
	}
	
	public Object getData() {
		return m_data;
	}

	public void setData(Object data) {
		m_data = data;
	}

	@JsonIgnore
	public String getEventSource() {
		return m_eventSource;
	}

	public void setEventSource(String eventSource) {
		m_eventSource = eventSource;
	}

	@Override
	public String getEventType() {
		return null;
	}
}
