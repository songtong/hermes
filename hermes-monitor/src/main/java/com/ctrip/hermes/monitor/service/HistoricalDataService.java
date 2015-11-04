package com.ctrip.hermes.monitor.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;

import com.ctrip.hermes.monitor.Bootstrap;

@Service
public class HistoricalDataService {

	private static final Logger logger = LoggerFactory.getLogger(HistoricalDataService.class);

	@Autowired
	private ApplicationContext context;

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

	public List<String> fixPastHours(int hours, int requestIntervalSecond) throws Throwable {
		Map<String, IZabbixMonitor> beans = context.getBeansOfType(IZabbixMonitor.class);
		List<String> result = new ArrayList<String>(beans.size());
		for (Map.Entry<String, IZabbixMonitor> entry : beans.entrySet()) {
			logger.info("Fixing: " + entry.getValue().getClass());
			IZabbixMonitor zabbixMonitor = entry.getValue();
			String itemResult = zabbixMonitor.monitorPastHours(hours, requestIntervalSecond);
			result.add(itemResult);
		}
		return result;
	}

	@Async
	public Future<List<String>> fixPastHoursAsync(int hours, int requestIntervalSecond,DeferredResult<List<String>> deferredResult)
	      throws Throwable {
		List<String> fixPastHours = fixPastHours(hours, requestIntervalSecond);
		deferredResult.setResult(fixPastHours);
		return new AsyncResult<List<String>>(fixPastHours);
	}
}
