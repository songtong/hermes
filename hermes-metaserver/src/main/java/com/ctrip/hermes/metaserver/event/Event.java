package com.ctrip.hermes.metaserver.event;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Event {
	private EventType m_type;

	private long m_version;

	private Object m_data;

	private EventBus m_eventBus;

	private ClusterStateHolder m_stateHolder;

	public Event(EventType type, long version, ClusterStateHolder stateHolder, Object data) {
		m_type = type;
		m_version = version;
		m_stateHolder = stateHolder;
		m_data = data;
	}

	public ClusterStateHolder getStateHolder() {
		return m_stateHolder;
	}

	public EventBus getEventBus() {
		return m_eventBus;
	}

	public void setEventBus(EventBus eventBus) {
		m_eventBus = eventBus;
	}

	public EventType getType() {
		return m_type;
	}

	public long getVersion() {
		return m_version;
	}

	public Object getData() {
		return m_data;
	}

}
