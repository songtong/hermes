package com.ctrip.hermes.metaservice.monitor.dao;

import java.util.List;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;

public interface MonitorEventStorage {
	public void addMonitorEvent(MonitorEvent event) throws Exception;

	public List<MonitorEvent> findMonitorEvent(MonitorEventType type, long start, long end);

	public List<MonitorEvent> findMonitorEvent(long start, long end);
}
