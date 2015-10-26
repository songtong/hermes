package com.ctrip.hermes.monitor.zabbix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.stat.StatUtils;

import com.ctrip.hermes.monitor.stat.StatResult;
import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryGetRequest;
import com.zabbix4j.history.HistoryGetResponse;
import com.zabbix4j.history.HistoryObject;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.item.ItemGetRequest;
import com.zabbix4j.item.ItemGetResponse;
import com.zabbix4j.item.ItemGetResponse.Result;
import com.zabbix4j.item.ItemObject;

public class ZabbixStatUtils {

	public static Map<Integer, HistoryObject> getHistory(Date timeFrom, Date timeTill, Integer hostid,
	      List<ItemObject> items, HISOTRY_OBJECT_TYPE hisotry) throws ZabbixApiException {
		HistoryGetRequest historyGetRequest = new HistoryGetRequest();
		historyGetRequest.getParams().setHistory(hisotry.value);
		historyGetRequest.getParams().setHostids(Arrays.asList(hostid));
		List<Integer> itemids = new ArrayList<Integer>();
		for (ItemObject item : items) {
			itemids.add(item.getItemid());
		}
		historyGetRequest.getParams().setItemids(itemids);
		historyGetRequest.getParams().setSortField("itemid");
		historyGetRequest.getParams().setTime_from(timeFrom.getTime() / 1000);
		historyGetRequest.getParams().setTime_till(timeTill.getTime() / 1000);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		HistoryGetResponse hisotryGetResponse = zabbixApi.history().get(historyGetRequest);

		Map<Integer, HistoryObject> result = new HashMap<Integer, HistoryObject>();
		for (HistoryObject ho : hisotryGetResponse.getResult()) {
			result.put(ho.getItemid(), ho);
		}
		return result;
	}

	public static Map<Integer, StatResult> getHistoryStat(Date timeFrom, Date timeTill, Integer hostid,
	      List<ItemObject> items, HISOTRY_OBJECT_TYPE history) throws ZabbixApiException {
		List<Integer> itemids = new ArrayList<Integer>();
		for (ItemObject item : items) {
			itemids.add(item.getItemid());
		}

		HistoryGetRequest historyGetRequest = new HistoryGetRequest();
		historyGetRequest.getParams().setHistory(history.value);
		historyGetRequest.getParams().setHostids(Arrays.asList(hostid));
		historyGetRequest.getParams().setItemids(itemids);
		historyGetRequest.getParams().setSortField("clock");
		historyGetRequest.getParams().setTime_from(timeFrom.getTime() / 1000);
		historyGetRequest.getParams().setTime_till(timeTill.getTime() / 1000);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		HistoryGetResponse hisotryGetResponse = zabbixApi.history().get(historyGetRequest);

		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		Map<Integer, List<Double>> values = new HashMap<Integer, List<Double>>();
		for (HistoryObject ho : hisotryGetResponse.getResult()) {
			if (!values.containsKey(ho.getItemid())) {
				values.put(ho.getItemid(), new ArrayList<Double>());
			}
			List<Double> list = values.get(ho.getItemid());
			list.add(Double.parseDouble(ho.getValue()));
		}
		for (Map.Entry<Integer, List<Double>> entry : values.entrySet()) {
			double[] array = new double[entry.getValue().size()];
			for (int i = 0; i < array.length; i++) {
				array[i] = entry.getValue().get(i);
			}
			double mean = StatUtils.mean(array);
			double max = StatUtils.max(array);
			double min = StatUtils.min(array);
			double sum = StatUtils.sum(array);
			double variance = StatUtils.variance(array);
			StatResult statResult = new StatResult();
			statResult.setMax(max);
			statResult.setMean(mean);
			statResult.setMin(min);
			statResult.setSum(sum);
			statResult.setVariance(variance);
			result.put(entry.getKey(), statResult);
		}
		return result;
	}

	public static Map<Integer, Result> getItems(Integer hostid, List<ItemObject> items) throws ZabbixApiException {
		ItemGetRequest itemGetRequest = new ItemGetRequest();
		itemGetRequest.getParams().setHostids(Arrays.asList(hostid));
		List<Integer> itemids = new ArrayList<Integer>();
		for (ItemObject item : items) {
			itemids.add(item.getItemid());
		}
		itemGetRequest.getParams().setItemids(itemids);
		itemGetRequest.getParams().setSortField("key_");

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse itemGetResponse = zabbixApi.item().get(itemGetRequest);

		Map<Integer, Result> result = new HashMap<Integer, Result>();
		for (Result r : itemGetResponse.getResult()) {
			result.put(r.getItemid(), r);
		}
		return result;
	}
}
