package com.ctrip.hermes.monitor.rest;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ctrip.hermes.monitor.domain.MonitorItem;
import com.ctrip.hermes.monitor.service.ESMonitorService;
import com.ctrip.hermes.monitor.service.HistoricalDataService;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;

@RestController
@RequestMapping("zabbix")
public class ZabbixMonitorController {

	@Autowired
	private ESMonitorService monitorService;

	@Autowired
	private HistoricalDataService fixService;

	@RequestMapping(value = "latest", method = RequestMethod.GET)
	public String latest() {
		MonitorItem cpuItem = monitorService.queryLatestMonitorItem(ZabbixConst.CATEGORY_CPU);
		MonitorItem memoryItem = monitorService.queryLatestMonitorItem(ZabbixConst.CATEGORY_MEMORY);
		MonitorItem diskItem = monitorService.queryLatestMonitorItem(ZabbixConst.CATEGORY_DISK);
		MonitorItem kafkaItem = monitorService.queryLatestMonitorItem(ZabbixConst.CATEGORY_KAFKA);
		MonitorItem zkItem = monitorService.queryLatestMonitorItem(ZabbixConst.CATEGORY_ZK);
		List<MonitorItem> result = new ArrayList<MonitorItem>();
		result.add(cpuItem);
		result.add(memoryItem);
		result.add(diskItem);
		result.add(kafkaItem);
		result.add(zkItem);
		return JSON.toJSONString(result, SerializerFeature.WriteDateUseDateFormat);
	}

	@RequestMapping(value = "fixhours", method = RequestMethod.POST)
	public DeferredResult<List<String>> fixHours(@RequestParam(value = "hours") int hours) throws Throwable {
		int requestIntervalSecond = 5;
		DeferredResult<List<String>> deferredResult = new DeferredResult<List<String>>();
		fixService.fixPastHoursAsync(hours, requestIntervalSecond, deferredResult);
		return deferredResult;
	}
}
