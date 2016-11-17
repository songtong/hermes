package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public class MetaServerErrorEvent extends ServerErrorEvent {

	public MetaServerErrorEvent() {
		this(null, -1);
	}

	public MetaServerErrorEvent(String host, long errorCount) {
		super(MonitorEventType.METASERVER_ERROR, host, errorCount);
	}

	@Override
	public String toString() {
		return "MetaServerErrorEvent [m_host=" + getHost() + ", m_errorCount=" + getErrorCount() + "]";
	}

	@Override
	String getMessageFormat() {
		return "[%s] MetaServer %s has got %s times error.";
	}
}
