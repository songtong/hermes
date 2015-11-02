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
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		MemoryMonitor monitor = context.getBean(MemoryMonitor.class);
		monitor.monitorPastHours(48, 5);
		context.close();
	}

	@Autowired
	private ESMonitorService service;

	@Autowired
	private ZabbixApiGateway zabbixApi;

	@Autowired
	private MonitorConfig config;

	private void monitorMemory(Date timeFrom, Date timeTill, String group, String[] hostNames) throws Throwable {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(hostNames);
		Map<Integer, StatResult> availablePercentage = statAvailablePercentage(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> freeSwapPercentage = statFreeSwapPercentage(timeFrom, timeTill, hosts);

		for (Integer hostid : hosts.keySet()) {
			Map<String, Object> stat = new HashMap<String, Object>();
			stat.put("memory.available.percentage", availablePercentage.get(hostid).getMean());
			stat.put("memory.freeswap.percentage", freeSwapPercentage.get(hostid).getMean());

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

	@Scheduled(cron = "0 6 * * * *")
	public void monitorHourly() throws Throwable {
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
	}

	public void monitorPastHours(int hours, int requestIntervalSecond) throws Throwable {
		for (int i = hours - 1; i >= 0; i--) {
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.add(Calendar.HOUR_OF_DAY, -i);
			Date timeTill = cal.getTime();
			cal.add(Calendar.HOUR_OF_DAY, -1);
			Date timeFrom = cal.getTime();

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
	}

	private Map<Integer, StatResult> statAvailablePercentage(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
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

	private Map<Integer, StatResult> statFreeSwapPercentage(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
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

}
