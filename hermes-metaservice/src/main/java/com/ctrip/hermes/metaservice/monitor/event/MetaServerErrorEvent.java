package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class MetaServerErrorEvent extends AbstractMonitorEvent {

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
		return MonitorEventType.METASERVER_ERROR;
	}
}
