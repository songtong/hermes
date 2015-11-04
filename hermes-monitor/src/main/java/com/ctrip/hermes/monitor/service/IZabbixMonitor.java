package com.ctrip.hermes.monitor.service;

public interface IZabbixMonitor {
	public void scheduled() throws Throwable;

	public String monitorHourly() throws Throwable;

	public String monitorPastHours(int hours, int requestIntervalSecond) throws Throwable;
}
