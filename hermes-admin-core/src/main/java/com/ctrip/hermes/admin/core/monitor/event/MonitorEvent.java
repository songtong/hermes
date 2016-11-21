package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public interface MonitorEvent {
	public boolean isShouldNotify();

	public void setShouldNotify(boolean shouldNotify);

	public MonitorEventType getType();

	public com.ctrip.hermes.admin.core.model.MonitorEvent toDBEntity();

	public void parse(com.ctrip.hermes.admin.core.model.MonitorEvent eventDao);
}
