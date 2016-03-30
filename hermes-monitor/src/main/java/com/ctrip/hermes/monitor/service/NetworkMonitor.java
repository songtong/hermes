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
public class NetworkMonitor implements IZabbixMonitor {

	private static final Logger logger = LoggerFactory.getLogger(NetworkMonitor.class);

	public static void main(String[] args) throws Throwable {
		int hours = Integer.parseInt(args[0]);
		int requestIntervalSecond = Integer.parseInt(args[1]);
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		NetworkMonitor monitor = context.getBean(NetworkMonitor.class);
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

		monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
		monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
		monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
		monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
		monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
		return String.format("%s: %s->%s", "CPU", timeFrom, timeTill);
	}

	private void monitorNetwork(Date timeFrom, Date timeTill, String group, String[] hostNames) throws Throwable {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(hostNames);
		Map<Integer, Map<String, StatResult>> incomingTraffic = monitorNetworkIncomingTraffic(timeFrom, timeTill, hosts);
		Map<Integer, Map<String, StatResult>> outgoingTraffic = monitorNetworkOutgoingTraffic(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> networkEstablished = statNetworkEstablished(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> networkSyncRecv = statNetworkSyncRecv(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> networkTimeWait = statNetworkTimeWait(timeFrom, timeTill, hosts);

		for (Integer hostid : hosts.keySet()) {
			Map<String, Object> stat = new HashMap<String, Object>();
			Map<String, StatResult> resultMap = incomingTraffic.get(hostid);
			for (Map.Entry<String, StatResult> entry : resultMap.entrySet()) {
				stat.put(entry.getKey(), entry.getValue().getMean());
			}
			resultMap = outgoingTraffic.get(hostid);
			for (Map.Entry<String, StatResult> entry : resultMap.entrySet()) {
				stat.put(entry.getKey(), entry.getValue().getMean());
			}

			stat.put("network.established", networkEstablished.get(hostid).getMean());
			stat.put("network.syncrecv", networkSyncRecv.get(hostid).getMean());
			stat.put("network.timewait", networkTimeWait.get(hostid).getMean());

			MonitorItem item = new MonitorItem();
			item.setCategory(ZabbixConst.CATEGORY_NETWORK);
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

	private Map<Integer, Map<String, StatResult>> monitorNetworkIncomingTraffic(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.NETWORK_INCOMING_TRAFFIC);
		Map<Integer, ItemObject> items = new HashMap<Integer, ItemObject>();
		for (List<ItemObject> list : ids.values()) {
			for (ItemObject io : list) {
				items.put(io.getItemid(), io);
			}
		}
		Map<Integer, Map<String, StatResult>> result = new HashMap<Integer, Map<String, StatResult>>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			Map<String, StatResult> validResults = new HashMap<String, StatResult>();
			result.put(hostid, validResults);
			for (Map.Entry<Integer, StatResult> entry : statResults.entrySet()) {
				if (entry.getValue().getSum() > 0) {
					String name = items.get(entry.getKey()).getKey_();
					validResults.put(name, entry.getValue());
					logger.info(String.format("%14s Incoming Traffic %s(%s - %s) Mean: %,9.0f ",
					      hosts.get(hostid).getHost(), name, timeFrom, timeTill, entry.getValue().getMean()));
				}
			}
		}
		return result;
	}

	private Map<Integer, Map<String, StatResult>> monitorNetworkOutgoingTraffic(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.NETWORK_OUTGOING_TRAFFIC);
		Map<Integer, ItemObject> items = new HashMap<Integer, ItemObject>();
		for (List<ItemObject> list : ids.values()) {
			for (ItemObject io : list) {
				items.put(io.getItemid(), io);
			}
		}
		Map<Integer, Map<String, StatResult>> result = new HashMap<Integer, Map<String, StatResult>>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			Map<String, StatResult> validResults = new HashMap<String, StatResult>();
			result.put(hostid, validResults);
			for (Map.Entry<Integer, StatResult> entry : statResults.entrySet()) {
				if (entry.getValue().getSum() > 0) {
					String name = items.get(entry.getKey()).getKey_();
					validResults.put(name, entry.getValue());
					logger.info(String.format("%14s Incoming Traffic %s(%s - %s) Mean: %,9.0f ",
					      hosts.get(hostid).getHost(), name, timeFrom, timeTill, entry.getValue().getMean()));
				}
			}
		}
		return result;
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

			monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_KAFKA_BROKER, config.getZabbixKafkaBrokerHosts());
			monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_ZOOKEEPER, config.getZabbixZookeeperHosts());
			monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_MYSQL_BROKER, config.getZabbixMysqlBrokerHosts());
			monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_METASERVER, config.getZabbixMetaserverHosts());
			monitorNetwork(timeFrom, timeTill, ZabbixConst.GROUP_PORTAL, config.getZabbixPortalHosts());
			try {
				Thread.sleep(requestIntervalSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
		return String.format("%s->%s", firstTimeFrom, lastTimeTill);
	}

	@Scheduled(cron = "0 7 * * * *")
	public void scheduled() throws Throwable {
		monitorHourly();
	}

	private Map<Integer, StatResult> statNetworkEstablished(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.NETWORK_ESTABLISHED);
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
			logger.info(String.format("%14s Network Established(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statNetworkSyncRecv(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.NETWORK_SYN_RECV);
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
			logger.info(String.format("%14s Network Sync Recv(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statNetworkTimeWait(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.NETWORK_TIME_WAIT);
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
			logger.info(String.format("%14s Network Time Wait(%s - %s) Mean: %,9.0f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}
}
