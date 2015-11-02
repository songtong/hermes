package com.ctrip.hermes.monitor.service;


public interface IZabbixMonitor {
	public void monitorHourly() throws Throwable;

	public void monitorPastHours(int hours, int requestIntervalSecond) throws Throwable;
}
