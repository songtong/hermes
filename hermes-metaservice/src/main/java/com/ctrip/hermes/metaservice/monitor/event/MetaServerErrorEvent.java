package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class MetaServerErrorEvent extends BaseMonitorEvent {

	public MetaServerErrorEvent() {
		super(MonitorEventType.METASERVER_ERROR);
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {

	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {

	}

}
