package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class BrokerErrorEvent implements MonitorEvent {

	private String m_brokerIp;

	@Override
	public MonitorEventType getType() {
		return MonitorEventType.BROKER_ERROR;
	}

	@Override
	public com.ctrip.hermes.metaservice.model.MonitorEvent toDBEntity() {
		com.ctrip.hermes.metaservice.model.MonitorEvent e = new com.ctrip.hermes.metaservice.model.MonitorEvent();
		return e;
	}

	@Override
	public MonitorEvent parse(com.ctrip.hermes.metaservice.model.MonitorEvent monitorEventDal) {
		return null;
	}
}
