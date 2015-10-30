package com.ctrip.hermes.monitor.zabbix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.host.HostGetRequest;
import com.zabbix4j.host.HostGetResponse;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.hostgroup.HostgroupGetRequest;
import com.zabbix4j.hostgroup.HostgroupGetRequest.Filter;
import com.zabbix4j.hostgroup.HostgroupGetResponse;
import com.zabbix4j.item.ItemGetRequest;
import com.zabbix4j.item.ItemGetResponse;
import com.zabbix4j.item.ItemObject;

public class ZabbixApiUtils {

	public static String DEFAULT_USER = "guest";

	public static String DEFAULT_PASSWORD = "";

	public static String DEFAULT_ZABBIX_URL = "http://zabbixserver.sh.ctripcorp.com/api_jsonrpc.php";

	private static ZabbixApi zabbixApi;

	public static Map<Integer, HostObject> getHosts(List<Integer> hostids) throws ZabbixApiException {
		HostGetRequest request = new HostGetRequest();
		request.getParams().setHostids(hostids);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		HostGetResponse response = zabbixApi.host().get(request);

		Map<Integer, HostObject> result = new HashMap<Integer, HostObject>();
		for (com.zabbix4j.host.HostGetResponse.Result host : response.getResult()) {
			result.put(host.getHostid(), host);
		}
		return result;
	}

	public static Map<Integer, ItemObject> getItems(List<Integer> itemids) throws ZabbixApiException {
		ItemGetRequest request = new ItemGetRequest();
		request.getParams().setItemids(itemids);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse response = zabbixApi.item().get(request);

		Map<Integer, ItemObject> result = new HashMap<Integer, ItemObject>();
		for (com.zabbix4j.item.ItemGetResponse.Result item : response.getResult()) {
			result.put(item.getItemid(), item);
		}
		return result;
	}

	// TODO enable configuration
	public static ZabbixApi getZabbixApiInstance() throws ZabbixApiException {
		if (zabbixApi == null) {
			zabbixApi = new ZabbixApi(DEFAULT_ZABBIX_URL);
			zabbixApi.login(DEFAULT_USER, DEFAULT_PASSWORD);
		}
		return zabbixApi;
	}

	public static Map<Integer, HostObject> searchHosts(String... hostGroup) throws ZabbixApiException {
		HostgroupGetRequest grequest = new HostgroupGetRequest();
		Filter filter = grequest.getParams().newFilter();
		for (String group : hostGroup) {
			filter.addName(group);
		}
		grequest.getParams().setFilter(filter);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		HostgroupGetResponse gresponse = zabbixApi.hostgroup().get(grequest);

		Map<Integer, HostObject> result = new HashMap<Integer, HostObject>();

		for (com.zabbix4j.hostgroup.HostgroupGetResponse.Result gr : gresponse.getResult()) {
			Integer groupId = gr.getGroupid();
			HostGetRequest request = new HostGetRequest();
			request.getParams().setGroupids(Arrays.asList(groupId));

			HostGetResponse response = zabbixApi.host().get(request);
			for (com.zabbix4j.host.HostGetResponse.Result hr : response.getResult()) {
				result.put(hr.getHostid(), hr);
			}
		}
		return result;
	}

	public static Map<Integer, List<ItemObject>> searchItemsByName(Collection<Integer> hostids, String searchName)
	      throws ZabbixApiException {
		ItemGetRequest request = new ItemGetRequest();
		List<Integer> hostidList = new ArrayList<Integer>();
		hostidList.addAll(hostids);
		request.getParams().setHostids(hostidList);
		Map<String, String> search = new HashMap<String, String>();
		search.put("name", searchName);
		request.getParams().setSearch(search);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse response = zabbixApi.item().get(request);

		Map<Integer, List<ItemObject>> result = new HashMap<Integer, List<ItemObject>>();
		for (com.zabbix4j.item.ItemGetResponse.Result r : response.getResult()) {
			if (!result.containsKey(r.getHostid())) {
				result.put(r.getHostid(), new ArrayList<ItemObject>());
			}
			List<ItemObject> ids = result.get(r.getHostid());
			ids.add(r);
		}
		return result;
	}

	public static Map<Integer, List<ItemObject>> searchItemsByKey(Collection<Integer> hostids, String keyName)
	      throws ZabbixApiException {
		ItemGetRequest request = new ItemGetRequest();
		List<Integer> hostidList = new ArrayList<Integer>();
		hostidList.addAll(hostids);
		request.getParams().setHostids(hostidList);
		Map<String, String> search = new HashMap<String, String>();
		search.put("key_", keyName);
		request.getParams().setSearch(search);

		ZabbixApi zabbixApi = ZabbixApiUtils.getZabbixApiInstance();
		ItemGetResponse response = zabbixApi.item().get(request);

		Map<Integer, List<ItemObject>> result = new HashMap<Integer, List<ItemObject>>();
		for (com.zabbix4j.item.ItemGetResponse.Result r : response.getResult()) {
			if (!result.containsKey(r.getHostid())) {
				result.put(r.getHostid(), new ArrayList<ItemObject>());
			}
			List<ItemObject> ids = result.get(r.getHostid());
			ids.add(r);
		}
		return result;
	}

	public static void main(String[] args) throws ZabbixApiException {
		List<String> groups = Arrays.asList(ZabbixConst.GROUP_KAFKA_BROKER, ZabbixConst.GROUP_ZOOKEEPER,
		      ZabbixConst.GROUP_MYSQL_BROKER, ZabbixConst.GROUP_METASERVER,
		      ZabbixConst.GROUP_PORTAL);
		for (String group : groups) {
			Map<Integer, HostObject> hosts = searchHosts(group);
			System.out.println(hosts);
		}
	}
}
