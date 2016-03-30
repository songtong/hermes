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
import com.ctrip.hermes.monitor.zabbix.ZabbixApiGateway1;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemObject;

@Service
public class ZKZabbixMonitor implements IZabbixMonitor {

	private static final Logger logger = LoggerFactory.getLogger(ZKZabbixMonitor.class);

	public static void main(String[] args) throws Throwable {
		int hours = Integer.parseInt(args[0]);
		int requestIntervalSecond = Integer.parseInt(args[1]);
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		ZKZabbixMonitor monitor = context.getBean(ZKZabbixMonitor.class);
		monitor.monitorPastHours(hours, requestIntervalSecond);
		context.close();
	}

	@Autowired
	private ESMonitorService service;

	@Autowired
	private ZabbixApiGateway1 zabbixApi;

	@Autowired
	private MonitorConfig config;

	@Scheduled(cron = "0 7 * * * *")
	public void scheduled() throws Throwable{
		monitorHourly();
	}
	
	public String monitorHourly() throws Throwable {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		Date timeFrom = cal.getTime();

		monitorZK(timeFrom, timeTill);
		return String.format("%s->%s", timeFrom, timeTill);
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
			
			monitorZK(timeFrom, timeTill);

			try {
				Thread.sleep(requestIntervalSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
		return String.format("%s: %s->%s","Zookeeper", firstTimeFrom, lastTimeTill);
	}

	private void monitorZK(Date timeFrom, Date timeTill) throws Throwable {
		Map<Integer, HostObject> hosts = zabbixApi.searchHostsByName(config.getZabbixZookeeperHosts());
		Map<Integer, StatResult> avgLatency = statAvgLatency(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> znodeCount = statZnodeCount(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> watchCount = statWatchCount(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> outstandingRequests = statOutstandingRequests(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> openFDCount = statOpenFDCount(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> aliveConnectionCount = statAliveConnectionCount(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> maxLatency = statMaxLatency(timeFrom, timeTill, hosts);
		Map<Integer, StatResult> ephemeralsCount = statEphemeralsCount(timeFrom, timeTill, hosts);

		for (Integer hostid : hosts.keySet()) {
			Map<String, Object> stat = new HashMap<String, Object>();
			stat.put("zk.avglatency", avgLatency.get(hostid).getMean());
			stat.put("zk.znodecount", znodeCount.get(hostid).getMean());
			stat.put("zk.watchcount", watchCount.get(hostid).getMean());
			stat.put("zk.outstandingrequests", outstandingRequests.get(hostid).getMean());
			stat.put("zk.openfdcount", openFDCount.get(hostid).getMean());
			stat.put("zk.aliveconnectioncount", aliveConnectionCount.get(hostid).getMean());
			stat.put("zk.maxlatency", maxLatency.get(hostid).getMean());
			stat.put("zk.ephemeralscount", ephemeralsCount.get(hostid).getMean());

			MonitorItem item = new MonitorItem();
			item.setCategory(ZabbixConst.CATEGORY_ZK);
			item.setSource(ZabbixConst.SOURCE_ZABBIX);
			item.setStartDate(timeFrom);
			item.setEndDate(timeTill);
			item.setHost(hosts.get(hostid).getHost());
			item.setGroup(ZabbixConst.GROUP_ZOOKEEPER);
			item.setValue(stat);

			try {
				IndexResponse response = service.prepareIndex(item);
				logger.info(response.getId());
			} catch (IOException e) {
				logger.warn("Save item failed", e);
			}
		}
	}

	private Map<Integer, StatResult> statAliveConnectionCount(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.ZK_NUM_ALIVE_CONNECTIONS);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getMean() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			logger.info(String.format("%14s Alive Connection(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statAvgLatency(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.ZK_AVG_LATENCY);
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
			logger.info(String.format("%14s Avg Latency(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statEphemeralsCount(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.ZK_EPHEMERALS_COUNT);
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
			logger.info(String.format("%14s Ephemerals Count(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statMaxLatency(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.ZK_MAX_LATENCY);
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
			logger.info(String.format("%14s Max Latency(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statOpenFDCount(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.ZK_OPEN_FD_COUNT);
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
			logger.info(String.format("%14s Open FD(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom, timeTill,
			      validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statOutstandingRequests(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.ZK_OUTSTANDING_REQUESTS);
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
			logger.info(String.format("%14s Outstanding Requests(%s - %s) Mean: %f", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statWatchCount(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.ZK_WATCH_COUNT);
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
			logger.info(String.format("%14s Znode Watch(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}

	private Map<Integer, StatResult> statZnodeCount(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.ZK_ZNODE_COUNT);
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
			logger.info(String.format("%14s Znode Count(%s - %s) Mean: %f", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, validResult.getMean()));
		}
		return result;
	}
}
