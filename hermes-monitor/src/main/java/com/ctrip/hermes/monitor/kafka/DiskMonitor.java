package com.ctrip.hermes.monitor.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.model.MonitorReport;
import com.ctrip.hermes.monitor.service.MonitorReportService;
import com.ctrip.hermes.monitor.stat.StatResult;
import com.ctrip.hermes.monitor.zabbix.ZabbixApiUtils;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;
import com.ctrip.hermes.monitor.zabbix.ZabbixStatUtils;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemGetResponse.Result;
import com.zabbix4j.item.ItemObject;

@Service
public class DiskMonitor {

	public static void main(String[] args) throws ZabbixApiException, DalException {
		DiskMonitor monitor = new DiskMonitor();
		monitor.service = new MonitorReportService();
		monitor.getPastHourDiskFreePercentage();
		// monitor.getCurrentDiskFreePercentage();
		// monitor.getPast5DaysDiskFreePercentage();
	}

	@Autowired
	private MonitorReportService service;

	public Map<Integer, Map<String, Double>> getCurrentDiskFreePercentage() throws ZabbixApiException {
		Map<Integer, HostObject> hosts = ZabbixApiUtils.searchHosts(ZabbixConst.GROUP_NAME_KAFKA);
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.DISK_FREE_PERCENTAGE);
		Map<Integer, Map<String, Double>> result = new HashMap<Integer, Map<String, Double>>();

		for (Integer hostid : hosts.keySet()) {
			System.out.format("%30s\n", hosts.get(hostid).getHost());
			Map<Integer, Result> items = ZabbixStatUtils.getItems(hostid, ids.get(hostid));
			Map<String, Double> diskFreeValue = new HashMap<String, Double>();
			for (Map.Entry<Integer, Result> item : items.entrySet()) {
				System.out.format("%30s\t%5.2f%%\n", item.getValue().getKey_(),
				      Double.parseDouble(item.getValue().getLastvalue()));
				diskFreeValue.put(item.getValue().getKey_(), Double.parseDouble(item.getValue().getLastvalue()));
			}
			result.put(hosts.get(hostid).getHostid(), diskFreeValue);
		}
		return result;
	}

	public void getPast5DaysDiskFreePercentage() throws ZabbixApiException {
		Map<Integer, HostObject> hosts = ZabbixApiUtils.searchHosts(ZabbixConst.GROUP_NAME_KAFKA);
		for (Integer hostid : hosts.keySet()) {
			System.out.format("Host: %s\n", hosts.get(hostid).getHost());
			getPast5DaysDiskFreePercentage(hostid);
		}
	}

	public void getPast5DaysDiskFreePercentage(Integer hostid) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> items = ZabbixApiUtils.searchItems(Arrays.asList(hostid),
		      ZabbixConst.DISK_FREE_PERCENTAGE);

		List<Date> header = new ArrayList<Date>();
		Map<Integer, List<HistoryObject>> fiveDaysData = new HashMap<Integer, List<HistoryObject>>();
		for (int day = 5; day >= 0; day--) {
			long timeFrom = System.currentTimeMillis() / 1000 - day * 24 * 60 * 60 - 120;
			long timeTill = timeFrom + 60;
			header.add(new Date(timeFrom * 1000));

			Map<Integer, HistoryObject> history = ZabbixStatUtils.getHistory(new Date(timeFrom * 1000), new Date(
			      timeTill * 1000), hostid, items.get(hostid), HISOTRY_OBJECT_TYPE.FLOAT);

			for (Map.Entry<Integer, HistoryObject> result : history.entrySet()) {
				if (!fiveDaysData.containsKey(result.getKey())) {
					fiveDaysData.put(result.getKey(), new ArrayList<HistoryObject>());
				}
				List<HistoryObject> itemValues = fiveDaysData.get(result.getKey());
				if (itemValues.size() > 0) {
					HistoryObject lastValue = itemValues.get(itemValues.size() - 1);
					if (Math.abs(result.getValue().getClock() - lastValue.getClock()) < 60 * 60) {
						continue;
					}
				}
				itemValues.add(result.getValue());
			}
		}

		System.out.format("%30s :", "Item");
		for (Date date : header) {
			System.out.format(" %1$tm-%1$td ", date);
		}
		System.out.format("%6s", "Avg");
		System.out.format("%7s", "Delta");
		System.out.format("%10s", "Comments");
		System.out.println();

		Map<Integer, ItemObject> itemObjects = new HashMap<Integer, ItemObject>();
		for (ItemObject item : items.get(hostid)) {
			itemObjects.put(item.getItemid(), item);
		}

		for (Map.Entry<Integer, List<HistoryObject>> entry : fiveDaysData.entrySet()) {
			Integer itemId = entry.getKey();
			List<HistoryObject> values = entry.getValue();
			Collections.sort(values, new Comparator<HistoryObject>() {

				public int compare(HistoryObject o1, HistoryObject o2) {
					return (int) (o1.getClock() - o2.getClock());
				}

			});

			System.out.format("%30s :", itemObjects.get(itemId).getKey_());
			double totalValue = 0.0;
			HistoryObject latestValue = null;
			for (HistoryObject history : values) {
				double value = Double.parseDouble(history.getValue());
				System.out.format("%1.2f%% ", value);
				totalValue += value;
				latestValue = history;
			}

			double avgValue = totalValue / values.size();
			System.out.format("%5.2f%% ", avgValue);
			System.out.format("%5.2f%% ", Double.parseDouble(latestValue.getValue()) - avgValue);
			StringBuilder comments = new StringBuilder();
			if (Double.parseDouble(latestValue.getValue()) < 20) {
				comments.append(String.format("%10s;", "Low Disk(<0.2)"));
			}
			System.out.print(comments);
			System.out.println();
		}
	}

	@Scheduled(fixedRate = 3600000)
	public void getPastHourDiskFreePercentage() throws ZabbixApiException, DalException {
		Map<Integer, HostObject> hosts = ZabbixApiUtils.searchHosts(ZabbixConst.GROUP_NAME_KAFKA);
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.DISK_FREE_PERCENTAGE);

		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
		Date timeFrom = cal.getTime();

		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> history = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.FLOAT);
			System.out.format("%30s %s-%s \n", hosts.get(hostid).getHost(), timeFrom, timeTill);
			Map<String, Object> stat = new HashMap<String, Object>();
			Map<Integer, ItemObject> items = new HashMap<Integer, ItemObject>();
			for (ItemObject item : ids.get(hostid)) {
				items.put(item.getItemid(), item);
			}
			for (Map.Entry<Integer, StatResult> h : history.entrySet()) {
				String name = items.get(h.getKey()).getKey_();
				stat.put(name, h.getValue().getMean());
				System.out.format("%30s : %5.2f%% \n", name, h.getValue().getMean());
			}

			String json = JSON.toJSONString(stat);
			MonitorReport report = new MonitorReport();
			report.setCategory("Disk");
			report.setSource("Zabbix");
			report.setStart(timeFrom);
			report.setEnd(timeTill);
			report.setHost(hosts.get(hostid).getHost());
			report.setValue(json);

			service.insertOrUpdate(report);
		}
	}
}
