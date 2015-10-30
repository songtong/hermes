package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class BrokerErrorEvent extends ServerErrorEvent {

	public BrokerErrorEvent() {
		this(null, -1);
	}

	public BrokerErrorEvent(String host, long errorCount) {
		super(MonitorEventType.BROKER_ERROR, host, errorCount);
	}

	@Override
	public String toString() {
		return "BrokerErrorEvent [m_host=" + getHost() + ", m_errorCount=" + getErrorCount() + "]";
	}

	@Override
	String getMessageFormat() {
		return "[%s] Broker %s has got %s times error.";
	}
}
