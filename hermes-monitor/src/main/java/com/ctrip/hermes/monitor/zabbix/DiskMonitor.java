package com.ctrip.hermes.monitor.zabbix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryGetRequest;
import com.zabbix4j.history.HistoryGetResponse;
import com.zabbix4j.history.HistoryObject;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.item.ItemGetRequest;
import com.zabbix4j.item.ItemGetResponse;
import com.zabbix4j.item.ItemGetResponse.Result;

public class DiskMonitor {
	public static void main(String[] args) throws IOException, ZabbixApiException {
		 getCurrentDiskFreePercentage();
//		getPast5DaysDiskFreePercentage();
	}

	public static void getPast5DaysDiskFreePercentage() throws ZabbixApiException {
		List<Integer> hostids = ZabbixIds.Kafka_Broker_Hostids;
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		for (Integer hostid : hostids) {
			System.out.format("Host: %s\n", hosts.get(hostid).getHost());
			getPast5DaysDiskFreePercentage(hostid);
		}
	}

	public static void getPast5DaysDiskFreePercentage(Integer hostid) throws ZabbixApiException {
		List<Integer> itemids = ZabbixIds.Disk_Free_Percentage_Itemids;
		Map<Integer, Result> items = ZabbixApiUtils.getItems(itemids);

		List<Date> header = new ArrayList<Date>();
		Map<Integer, List<HistoryObject>> fiveDaysData = new HashMap<Integer, List<HistoryObject>>();
		for (int day = 5; day >= 0; day--) {
			HistoryGetRequest historyGetRequest = new HistoryGetRequest();
			historyGetRequest.getParams().setHistory(HISOTRY_OBJECT_TYPE.FLOAT.value);
			historyGetRequest.getParams().setHostids(Arrays.asList(hostid));
			historyGetRequest.getParams().setItemids(itemids);
			historyGetRequest.getParams().setSortField("itemid");
			long timeFrom = System.currentTimeMillis() / 1000 - day * 24 * 60 * 60 - 120;
			long timeTill = timeFrom + 60;

			header.add(new Date(timeFrom * 1000));
			historyGetRequest.getParams().setTime_from(timeFrom);
			historyGetRequest.getParams().setTime_till(timeTill);

			ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
			HistoryGetResponse hisotryGetResponse = zabbixApi.history().get(historyGetRequest);

			for (HistoryObject result : hisotryGetResponse.getResult()) {
				if (!fiveDaysData.containsKey(result.getItemid())) {
					fiveDaysData.put(result.getItemid(), new ArrayList<HistoryObject>());
				}
				List<HistoryObject> itemValues = fiveDaysData.get(result.getItemid());
				if (itemValues.size() > 0) {
					HistoryObject lastValue = itemValues.get(itemValues.size() - 1);
					if (Math.abs(result.getClock() - lastValue.getClock()) < 60 * 60) {
						continue;
					}
				}
				itemValues.add(result);
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

	public static void getCurrentDiskFreePercentage() throws ZabbixApiException {
		List<Integer> hostids = ZabbixIds.Kafka_Broker_Hostids;
		List<Integer> itemids = ZabbixIds.Disk_Free_Percentage_Itemids;

		ItemGetRequest itemGetRequest = new ItemGetRequest();
		itemGetRequest.getParams().setHostids(hostids);
		itemGetRequest.getParams().setItemids(itemids);
		itemGetRequest.getParams().setSortField("key_");

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse itemGetResponse = zabbixApi.item().get(itemGetRequest);

		StringBuilder header = new StringBuilder(String.format("%30s\t", "Host"));
		for (Integer hostid : hostids) {
			header.append(String.format("%6s\t", hostid));
		}
		System.out.println(header);

		int columnIndex = 0;
		StringBuilder row = null;
		for (Result result : itemGetResponse.getResult()) {
			if (columnIndex++ == 0) {
				row = new StringBuilder();
				row.append(String.format("%30s\t", result.getKey_()));
			}
			row.append(String.format("%5.2f%%\t", Double.parseDouble(result.getLastvalue())));
			if (columnIndex == hostids.size()) {
				System.out.println(row);
				columnIndex = 0;
			}
		}
	}
}
