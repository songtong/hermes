package com.ctrip.hermes.monitor.service;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.monitor.Bootstrap;
import com.zabbix4j.ZabbixApiException;

public class FixHistoricalData {

	public static void main(String[] args) throws ZabbixApiException, DalException {
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		int hours = 48;
		int interval = 5;
		CPUMonitor cpuMonitor = context.getBean(CPUMonitor.class);
		cpuMonitor.monitorPastHours(hours, interval);
		DiskMonitor diskMonitor = context.getBean(DiskMonitor.class);
		diskMonitor.monitorPastHours(hours, interval);
		KafkaMonitor kafkaMonitor = context.getBean(KafkaMonitor.class);
		kafkaMonitor.monitorPastHours(hours, interval);
		ZKMonitor zkMonitor = context.getBean(ZKMonitor.class);
		zkMonitor.monitorPastHours(hours, interval);
		context.close();
	}

}
