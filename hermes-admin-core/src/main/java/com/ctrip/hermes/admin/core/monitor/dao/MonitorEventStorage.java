package com.ctrip.hermes.admin.core.monitor.dao;

import java.util.List;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;

public interface MonitorEventStorage {
	public void addMonitorEvent(MonitorEvent event) throws Exception;

	public List<MonitorEvent> findMonitorEvent(MonitorEventType type, long start, long end);

	public List<MonitorEvent> findMonitorEvent(long start, long end);

	public List<MonitorEvent> fetchUnnotifiedMonitorEvent(boolean isForNotify);

	public List<com.ctrip.hermes.admin.core.model.MonitorEvent> findDBMonitorEvents(int pageCount, int pageNum);

	public long totalPageCount(int pageSize);
}
