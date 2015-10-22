package com.ctrip.hermes.monitor.zabbix;

import java.util.Arrays;
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

public class ZabbixStatUtils {

	public static Map<Integer, HistoryObject> getHistory(Date timeFrom, Date timeTill, Integer hostid,
	      List<Integer> itemids, HISOTRY_OBJECT_TYPE hisotry) throws ZabbixApiException {
		HistoryGetRequest historyGetRequest = new HistoryGetRequest();
		historyGetRequest.getParams().setHistory(hisotry.value);
		historyGetRequest.getParams().setHostids(Arrays.asList(hostid));
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

	public static long getHistorySumByMinute(Date timeFrom, Date timeTill, Integer hostid, List<Integer> itemids,
	      HISOTRY_OBJECT_TYPE history) throws ZabbixApiException {
		HistoryGetRequest historyGetRequest = new HistoryGetRequest();
		historyGetRequest.getParams().setHistory(history.value);
		historyGetRequest.getParams().setHostids(Arrays.asList(hostid));
		historyGetRequest.getParams().setItemids(itemids);
		historyGetRequest.getParams().setSortField("clock");
		historyGetRequest.getParams().setTime_from(timeFrom.getTime() / 1000);
		historyGetRequest.getParams().setTime_till(timeTill.getTime() / 1000);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		HistoryGetResponse hisotryGetResponse = zabbixApi.history().get(historyGetRequest);

		long sum = 0;
		for (HistoryObject ho : hisotryGetResponse.getResult()) {
			long messagePerSec = Integer.parseInt(ho.getValue());
			sum += (messagePerSec * 60);
		}
		return sum;
	}

	public static Map<Integer, Result> getItems(Integer hostid, List<Integer> itemids) throws ZabbixApiException {
		ItemGetRequest itemGetRequest = new ItemGetRequest();
		itemGetRequest.getParams().setHostids(Arrays.asList(hostid));
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
