package com.ctrip.hermes.monitor.service;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.ctrip.hermes.monitor.Bootstrap;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.domain.MonitorItem;
import com.ctrip.hermes.monitor.stat.StatResult;
import com.ctrip.hermes.monitor.zabbix.ZabbixApiGateway;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemObject;

@Service
public class CPUMonitor implements IZabbixMonitor {

	private static final Logger logger = LoggerFactory.getLogger(CPUMonitor.class);

	public static void main(String[] args) throws Throwable {
		int hours = Integer.parseInt(args[0]);
		int requestIntervalSecond = Integer.parseInt(args[1]);
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		CPUMonitor monitor = context.getBean(CPUMonitor.class);
		monitor.monitorPastHours(hours, requestIntervalSecond);
		context.close();
	}

	@Autowired
	private ESMonitorService service;

	@Autowired
	@Qualifier("zabbixApiGateway")
	private ZabbixApiGateway zabbixApi;

	@Autowired
	private MonitorConfig config;

	private void monitorCPU(Date timeFrom, Date timeTill, String group, String[] hostNames) throws Throwable {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(hostNames);
		Map<Integer, StatResult> cpuUserTime = statCPUUserTime(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> cpuSystemTime = statCPUSystemTime(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> cpuIOWaitTime = statCPUIOWaitTime(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> cpuIdleTime = statCPUIdleTime(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> cpuProcessorLoad = statCPUProcessorLoad(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> cpuRatioLoadOfProcessor = statCPURatioLoadOfNumber(timeFrom, timeTill, hosts);

		for (Integer hostid : hosts.keySet()) {
			Map<String, Object> stat = new HashMap<String, Object>();
			stat.put("cpu.usertime", cpuUserTime.get(hostid).getMean() / 100);
			stat.put("cpu.systemtime", cpuSystemTime.get(hostid).getMean() / 100);
			stat.put("cpu.iowaittime", cpuIOWaitTime.get(hostid).getMean() / 100);
			stat.put("cpu.idletime", cpuIdleTime.get(hostid).getMean() / 100);
			stat.put("cpu.load", cpuProcessorLoad.get(hostid).getMean());
			stat.put("cpu.ratioloadofprocessor", cpuRatioLoadOfProcessor.get(hostid).getMean());
			MonitorItem item = new MonitorItem();
			item.setCategory(ZabbixConst.CATEGORY_CPU);
			item.setSource(ZabbixConst.SOURCE_ZABBIX);
			item.setStartDate(timeFrom);
			item.setEndDate(timeTill);
			item.setHost(hosts.get(hostid).getHost());
			item.setGroup(group);
			item.setValue(stat);

			try {
				IndexResponse response = service.prepareIndex(item);
				logger.info(response.getId());
			} catch (IOException e) {
				logger.warn("Save item failed", e);
			}
		}
	}

	public String monitorHourly() throws Throwable {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		Date timeFrom = cal.getTime();

		monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
		monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
		monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
		monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
		monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
		return String.format("%s: %s->%s", "CPU", timeFrom, timeTill);
	}

	public String monitorPastHours(int hours, int requestIntervalSecond) throws Throwable {
		Date firstTimeFrom = null;
		Date lastTimeTill = null;
		for (int i = hours - 1; i >= 0; i--) {
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.add(Calendar.HOUR_OF_DAY, -i);
			Date timeTill = cal.getTime();
			cal.add(Calendar.HOUR_OF_DAY, -1);
			Date timeFrom = cal.getTime();

			if (firstTimeFrom == null) {
				firstTimeFrom = timeFrom;
			}
			lastTimeTill = timeTill;

			monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
			monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
			monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
			monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
			monitorCPU(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
			try {
				Thread.sleep(requestIntervalSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
		return String.format("%s->%s", firstTimeFrom, lastTimeTill);
	}

	@Scheduled(cron = "0 3 * * * *")
	public void scheduled() throws Throwable {
		if (config.isMonitorCheckerEnable()) {
			monitorHourly();
		}
	}

	private Map<Integer, StatResult> statCPUIdleTime(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByKey(hosts.keySet(), ZabbixConst.CPU_IDLE_TIME);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s CPU Idle Time(%s - %s) Mean: %5.2f%%", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statCPUIOWaitTime(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByKey(hosts.keySet(), ZabbixConst.CPU_IOWAIT_TIME);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s CPU IOWait Time(%s - %s) Mean: %5.2f%%", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statCPUProcessorLoad(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.CPU_PROCESSOR_LOAD);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s CPU Processor Load(%s - %s) Mean: %5.2f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statCPURatioLoadOfNumber(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.CPU_RATIO_OF_CPU_LOAD_AND_CPU_NUMBER);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s CPU Ratio Load Of Processor Number(%s - %s) Mean: %5.2f", hosts.get(hostid)
			      .getHost(), timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statCPUSystemTime(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByKey(hosts.keySet(), ZabbixConst.CPU_SYSTEM_TIME);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s CPU System Time(%s - %s) Mean: %5.2f%%", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statCPUUserTime(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByKey(hosts.keySet(), ZabbixConst.CPU_USER_TIME);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s CPU User Time(%s - %s) Mean: %5.2f%%", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}
}
