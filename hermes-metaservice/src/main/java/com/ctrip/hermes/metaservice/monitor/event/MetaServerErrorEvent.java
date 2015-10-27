package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class MetaServerErrorEvent implements MonitorEvent {

	@Override
	public MonitorEventType getType() {
		return MonitorEventType.METASERVER_ERROR;
	}

	@Override
	public MonitorEvent parse(com.ctrip.hermes.metaservice.model.MonitorEvent monitorEventDal) {
		return null;
	}

	@Override
	public com.ctrip.hermes.metaservice.model.MonitorEvent toDBEntity() {
		return null;
	}
}
