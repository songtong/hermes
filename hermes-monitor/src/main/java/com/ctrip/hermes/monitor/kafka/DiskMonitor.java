package com.ctrip.hermes.monitor.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.ctrip.hermes.monitor.zabbix.ZabbixApiUtils;
import com.ctrip.hermes.monitor.zabbix.ZabbixIds;
import com.ctrip.hermes.monitor.zabbix.ZabbixStatUtils;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.item.ItemGetResponse.Result;

@Service
public class DiskMonitor {
	public static void main(String[] args) throws IOException, ZabbixApiException {
		DiskMonitor monitor = new DiskMonitor();
		// monitor.getPastHourDiskFreePercentage();
		monitor.getCurrentDiskFreePercentage();
		// monitor.getPast5DaysDiskFreePercentage();
	}

	public void getCurrentDiskFreePercentage() throws ZabbixApiException {
		List<Integer> hostids = ZabbixIds.Kafka_Broker_Hostids;
		List<Integer> itemids = ZabbixIds.Disk_Free_Percentage_Itemids;
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);

		for (Integer hostid : hostids) {
			System.out.format("%30s\n", hosts.get(hostid).getHost());
			Map<Integer, Result> items = ZabbixStatUtils.getItems(hostid, itemids);
			for (Map.Entry<Integer, Result> item : items.entrySet()) {
				System.out.format("%30s\t%5.2f%%\n", item.getValue().getKey_(),
				      Double.parseDouble(item.getValue().getLastvalue()));
			}
		}
	}

	public void getPast5DaysDiskFreePercentage() throws ZabbixApiException {
		List<Integer> hostids = ZabbixIds.Kafka_Broker_Hostids;
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		for (Integer hostid : hostids) {
			System.out.format("Host: %s\n", hosts.get(hostid).getHost());
			getPast5DaysDiskFreePercentage(hostid);
		}
	}

	public void getPast5DaysDiskFreePercentage(Integer hostid) throws ZabbixApiException {
		List<Integer> itemids = ZabbixIds.Disk_Free_Percentage_Itemids;
		Map<Integer, Result> items = ZabbixApiUtils.getItems(itemids);

		List<Date> header = new ArrayList<Date>();
		Map<Integer, List<HistoryObject>> fiveDaysData = new HashMap<Integer, List<HistoryObject>>();
		for (int day = 5; day >= 0; day--) {
			long timeFrom = System.currentTimeMillis() / 1000 - day * 24 * 60 * 60 - 120;
			long timeTill = timeFrom + 60;
			header.add(new Date(timeFrom * 1000));

			Map<Integer, HistoryObject> history = ZabbixStatUtils.getHistory(new Date(timeFrom * 1000), new Date(
			      timeTill * 1000), hostid, itemids, HISOTRY_OBJECT_TYPE.FLOAT);

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

		for (Map.Entry<Integer, List<HistoryObject>> entry : fiveDaysData.entrySet()) {
			Integer itemId = entry.getKey();
			List<HistoryObject> values = entry.getValue();
			Collections.sort(values, new Comparator<HistoryObject>() {

				public int compare(HistoryObject o1, HistoryObject o2) {
					return (int) (o1.getClock() - o2.getClock());
				}

			});

			System.out.format("%30s :", items.get(itemId).getKey_());
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
	public void getPastHourDiskFreePercentage() {

	}
}
