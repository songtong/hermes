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
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.monitor.Bootstrap;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.domain.MonitorItem;
import com.ctrip.hermes.monitor.stat.StatResult;
import com.ctrip.hermes.monitor.zabbix.ZabbixApiGateway;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemGetResponse.Result;
import com.zabbix4j.item.ItemObject;

@Service
public class DiskMonitor {

	private static final Logger logger = LoggerFactory.getLogger(DiskMonitor.class);

	public static void main(String[] args) throws ZabbixApiException, DalException {
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		DiskMonitor monitor = context.getBean(DiskMonitor.class);
		monitor.monitorPastHours(1, 5);
		context.close();
	}

	@Autowired
	private ESMonitorService service;

	@Autowired
	private ZabbixApiGateway zabbixApi;

	@Autowired
	private MonitorConfig config;

	public Map<Integer, Map<String, Double>> monitorCurrent() throws ZabbixApiException {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(config.getZabbixKafkaBrokerHosts());
		Map<Integer, List<ItemObject>> ids = zabbixApi
		      .searchItemsByName(hosts.keySet(), ZabbixConst.DISK_FREE_PERCENTAGE);
		Map<Integer, Map<String, Double>> result = new HashMap<Integer, Map<String, Double>>();

		for (Integer hostid : hosts.keySet()) {
			logger.info(String.format("%30s", hosts.get(hostid).getHost()));
			Map<Integer, Result> items = zabbixApi.getItems(hostid, ids.get(hostid));
			Map<String, Double> diskFreeValue = new HashMap<String, Double>();
			for (Map.Entry<Integer, Result> item : items.entrySet()) {
				logger.info(String.format("%30s\t%5.2f%%", item.getValue().getKey_(),
				      Double.parseDouble(item.getValue().getLastvalue())));
				diskFreeValue.put(item.getValue().getKey_(), Double.parseDouble(item.getValue().getLastvalue()));
			}
			result.put(hosts.get(hostid).getHostid(), diskFreeValue);
		}
		return result;
	}

	private void monitorDisk(Date timeFrom, Date timeTill, String group, String[] hostNames) throws ZabbixApiException {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(hostNames);
		Map<Integer, List<ItemObject>> ids = zabbixApi
		      .searchItemsByName(hosts.keySet(), ZabbixConst.DISK_FREE_PERCENTAGE);

		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> history = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			logger.info(String.format("%30s %s-%s ", hosts.get(hostid).getHost(), timeFrom, timeTill));
			Map<String, Object> stat = new HashMap<String, Object>();
			Map<Integer, ItemObject> items = new HashMap<Integer, ItemObject>();
			for (ItemObject item : ids.get(hostid)) {
				items.put(item.getItemid(), item);
			}
			for (Map.Entry<Integer, StatResult> h : history.entrySet()) {
				String name = items.get(h.getKey()).getKey_();
				stat.put(name, h.getValue().getMean());
				logger.info(String.format("%30s : %5.2f%% ", name, h.getValue().getMean()));
			}

			MonitorItem item = new MonitorItem();
			item.setCategory(ZabbixConst.CATEGORY_DISK);
			item.setSource(ZabbixConst.SOURCE_ZABBIX);
			item.setStartDate(timeFrom);
			item.setEndDate(timeTill);
			item.setHost(hosts.get(hostid).getHost());
			item.setGroup(group);
			item.setValue(stat);

			try {
				IndexResponse response = service.prepareIndex(item);
				logger.info(String.format("%s", response.getId()));
			} catch (IOException e) {
				logger.warn("Save item failed", e);
			}
		}
	}

	@Scheduled(cron = "0 4 * * * *")
	public void monitorHourly() throws ZabbixApiException, DalException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		Date timeFrom = cal.getTime();

		monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
		monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
		monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
		monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
		monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
	}

	public void monitorPastHours(int hours, int requestIntervalSecond) throws ZabbixApiException, DalException {
		for (int i = hours - 1; i >= 0; i--) {
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.add(Calendar.HOUR_OF_DAY, -i);
			Date timeTill = cal.getTime();
			cal.add(Calendar.HOUR_OF_DAY, -1);
			Date timeFrom = cal.getTime();

			monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
			monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
			monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
			monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
			monitorDisk(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
			try {
				Thread.sleep(requestIntervalSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
	}
}
