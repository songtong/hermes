package com.ctrip.hermes.monitor.zabbix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.host.HostGetRequest;
import com.zabbix4j.host.HostGetResponse;
import com.zabbix4j.item.ItemGetRequest;
import com.zabbix4j.item.ItemGetResponse;

public class ZabbixApiUtils {

	public static String DEFAULT_USER = "guest";

	public static String DEFAULT_PASSWORD = "";

	private static ZabbixApi zabbixApi;

	public static ZabbixApi getZabbixApiInstance() throws ZabbixApiException {
		if (zabbixApi == null) {
			String url = "http://zabbixserver.sh.ctripcorp.com/api_jsonrpc.php";
			zabbixApi = new ZabbixApi(url);
			zabbixApi.login(DEFAULT_USER, DEFAULT_PASSWORD);
		}
		return zabbixApi;
	}

	public static List<Integer> getItemIds(List<Integer> hostids, Map<String, String> search) throws ZabbixApiException {
		ItemGetRequest itemGetRequest = new ItemGetRequest();
		itemGetRequest.getParams().setHostids(hostids);
		itemGetRequest.getParams().setSearch(search);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse itemGetResponse = zabbixApi.item().get(itemGetRequest);

		List<Integer> ids = new ArrayList<Integer>();
		System.out.format("search %s \n", search);
		for (com.zabbix4j.item.ItemGetResponse.Result result : itemGetResponse.getResult()) {
			ids.add(result.getItemid());
		}
		System.out.println(ids);
		return ids;
	}

	public static Map<Integer, com.zabbix4j.item.ItemGetResponse.Result> getItems(List<Integer> itemids)
	      throws ZabbixApiException {
		ItemGetRequest itemGetRequest = new ItemGetRequest();
		itemGetRequest.getParams().setItemids(itemids);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse getResponse = zabbixApi.item().get(itemGetRequest);

		Map<Integer, com.zabbix4j.item.ItemGetResponse.Result> result = new HashMap<Integer, com.zabbix4j.item.ItemGetResponse.Result>();
		for (com.zabbix4j.item.ItemGetResponse.Result item : getResponse.getResult()) {
			result.put(item.getItemid(), item);
		}
		return result;
	}

	public static Map<Integer, com.zabbix4j.host.HostGetResponse.Result> getHosts(List<Integer> hostids)
	      throws ZabbixApiException {
		HostGetRequest hostGetRequest = new HostGetRequest();
		hostGetRequest.getParams().setHostids(hostids);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		HostGetResponse getResponse = zabbixApi.host().get(hostGetRequest);

		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> result = new HashMap<Integer, com.zabbix4j.host.HostGetResponse.Result>();
		for (com.zabbix4j.host.HostGetResponse.Result host : getResponse.getResult()) {
			result.put(host.getHostid(), host);
		}
		return result;
	}

	public static void main(String[] args) throws ZabbixApiException {
		Map<String, String> search = new HashMap<String, String>();
		search.put("name", args[0]);
		getItemIds(ZabbixIds.Kafka_Broker_Hostids, search);
	}
}
