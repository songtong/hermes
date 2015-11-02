package com.ctrip.hermes.monitor.service;

import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.ctrip.hermes.monitor.Bootstrap;

public class FixHistoricalData {

	public static void main(String[] args) throws Throwable {
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		int hours = Integer.parseInt(args[0]);
		int requestIntervalSecond = Integer.parseInt(args[1]);
		Map<String, IZabbixMonitor> beans = context.getBeansOfType(IZabbixMonitor.class);
		for (Map.Entry<String, IZabbixMonitor> entry : beans.entrySet()) {
			System.out.println(entry.getValue().getClass());
			IZabbixMonitor zabbixMonitor = entry.getValue();
			zabbixMonitor.monitorPastHours(hours, requestIntervalSecond);
		}
		context.close();
	}

}
