package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class BrokerErrorEvent extends AbstractMonitorEvent {

	private String m_brokerIp;

	@Override
	public com.ctrip.hermes.metaservice.model.MonitorEvent toMonitorEventDal() {
		return null;
	}

	@Override
	public MonitorEvent parse0(com.ctrip.hermes.metaservice.model.MonitorEvent monitorEventDal) {
		return null;
	}

	@Override
	public MonitorEventType getType() {
		return MonitorEventType.BROKER_ERROR;
	}

}
