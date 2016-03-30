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
public class MemoryMonitor implements IZabbixMonitor {

	private static final Logger logger = LoggerFactory.getLogger(MemoryMonitor.class);

	public static void main(String[] args) throws Throwable {
		int hours = Integer.parseInt(args[0]);
		int requestIntervalSecond = Integer.parseInt(args[1]);
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		MemoryMonitor monitor = context.getBean(MemoryMonitor.class);
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

	public String monitorHourly() throws Throwable {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		Date timeFrom = cal.getTime();

		monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
		monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
		monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
		monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
		monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
		return String.format("%s: %s->%s", "Memory", timeFrom, timeTill);
	}

	private void monitorMemory(Date timeFrom, Date timeTill, String group, String[] hostNames) throws Throwable {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(hostNames);
		Map<Integer, StatResult> availablePercentage = statMemoryAvailablePercentage(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> available = statMemoryAvailable(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> freeSwapPercentage = statMemoryFreeSwapPercentage(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> freeSwap = statMemoryFreeSwap(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> swapIn = statMemorySwapIn(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> swapOut = statMemorySwapOut(timeFrom, timeTill, hosts);

		for (Integer hostid : hosts.keySet()) {
			Map<String, Object> stat = new HashMap<String, Object>();
			stat.put("memory.available.percentage", availablePercentage.get(hostid).getMean() / 100);
			stat.put("memory.available", available.get(hostid).getMean());
			stat.put("memory.freeswap.percentage", freeSwapPercentage.get(hostid).getMean() / 100);
			stat.put("memory.freeswap", freeSwap.get(hostid).getMean());
			stat.put("memory.swapin", swapIn.get(hostid).getMean());
			stat.put("memory.swapout", swapOut.get(hostid).getMean());

			MonitorItem item = new MonitorItem();
			item.setCategory(ZabbixConst.CATEGORY_MEMORY);
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

			monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
			monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
			monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
			monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
			monitorMemory(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
			try {
				Thread.sleep(requestIntervalSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
		return String.format("%s->%s", firstTimeFrom, lastTimeTill);
	}

	@Scheduled(cron = "0 6 * * * *")
	public void scheduled() throws Throwable {
		monitorHourly();
	}

	private Map<Integer, StatResult> statMemoryAvailable(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.MEMORY_AVAILABLE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s Memory Available(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statMemoryAvailablePercentage(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.MEMORY_AVAILABLE_PERCENTAGE);
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
			logger.info(String.format("%14s Memory Available(%s - %s) Mean: %5.2f%%", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statMemoryFreeSwap(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.MEMORY_FREE_SWAP);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s Memory Free Swap(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statMemoryFreeSwapPercentage(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.MEMORY_FREE_SWAP_PERCENTAGE);
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
			logger.info(String.format("%14s Memory Free Swap(%s - %s) Mean: %5.2f%%", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statMemorySwapIn(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.MEMORY_SWAP_IN);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s Memory Swap In(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statMemorySwapOut(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.MEMORY_SWAP_OUT);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s Memory Swap Out(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}
}
