package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public interface MonitorEvent {

	public MonitorEventType getType();

	public com.ctrip.hermes.metaservice.model.MonitorEvent toDBEntity();

	public void parse(com.ctrip.hermes.metaservice.model.MonitorEvent eventDao);
}
