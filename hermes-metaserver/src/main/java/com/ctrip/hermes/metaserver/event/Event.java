package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Event {
	private EventType m_type;

	private Object m_data;

	public Event(EventType type, Object data) {
		m_type = type;
		m_data = data;
	}

	public EventType getType() {
		return m_type;
	}

	public Object getData() {
		return m_data;
	}

}
