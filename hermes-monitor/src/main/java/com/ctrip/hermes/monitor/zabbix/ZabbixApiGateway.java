package com.ctrip.hermes.monitor.zabbix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.math.stat.StatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.stat.StatResult;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryGetRequest;
import com.zabbix4j.history.HistoryGetResponse;
import com.zabbix4j.history.HistoryObject;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostGetRequest;
import com.zabbix4j.host.HostGetResponse;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemGetRequest;
import com.zabbix4j.item.ItemGetResponse;
import com.zabbix4j.item.ItemGetResponse.Result;
import com.zabbix4j.item.ItemObject;

@Service
@Scope("singleton")
public class ZabbixApiGateway {

	protected ZabbixApi zabbixApi;

	@Autowired
	protected MonitorConfig config;

	protected LoadingCache<String, Map<Integer, HostObject>> hostNameCache;

	@PostConstruct
	private void postConstruct() {
		if (config.isMonitorCheckerEnable()) {
			zabbixApi = new ZabbixApi(config.getZabbixUrl());
			try {
				zabbixApi.login(config.getZabbixUsername(), config.getZabbixPassword());
			} catch (ZabbixApiException e) {
				e.printStackTrace();
			}

			hostNameCache = CacheBuilder.newBuilder().recordStats().maximumSize(50).expireAfterWrite(1, TimeUnit.HOURS)
			      .build(new CacheLoader<String, Map<Integer, HostObject>>() {

				      @Override
				      public Map<Integer, HostObject> load(String name) throws Exception {
					      return populateHostsByName(name);
				      }

			      });
		}
	}

	public Map<Integer, HistoryObject> getHistory(Date timeFrom, Date timeTill, Integer hostid, List<ItemObject> items,
	      HISOTRY_OBJECT_TYPE hisotry) throws ZabbixApiException {
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

		HistoryGetResponse hisotryGetResponse = zabbixApi.history().get(historyGetRequest);

		Map<Integer, HistoryObject> result = new HashMap<Integer, HistoryObject>();
		for (HistoryObject ho : hisotryGetResponse.getResult()) {
			result.put(ho.getItemid(), ho);
		}
		return result;
	}

	public Map<Integer, StatResult> getHistoryStat(Date timeFrom, Date timeTill, Integer hostid, List<ItemObject> items,
	      HISOTRY_OBJECT_TYPE history) throws ZabbixApiException {
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		if (items == null) {
			return result;
		}
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

		HistoryGetResponse historyGetResponse = zabbixApi.history().get(historyGetRequest);

		Map<Integer, List<Double>> values = new HashMap<Integer, List<Double>>();
		for (HistoryObject ho : historyGetResponse.getResult()) {
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

	public Map<Integer, HostObject> getHosts(List<Integer> hostids) throws Throwable {
		HostGetRequest request = new HostGetRequest();
		request.getParams().setHostids(hostids);

		HostGetResponse response = zabbixApi.host().get(request);

		Map<Integer, HostObject> result = new HashMap<Integer, HostObject>();
		for (com.zabbix4j.host.HostGetResponse.Result host : response.getResult()) {
			result.put(host.getHostid(), host);
		}
		return result;
	}

	public Map<Integer, Result> getItems(Integer hostid, List<ItemObject> items) throws ZabbixApiException {
		ItemGetRequest itemGetRequest = new ItemGetRequest();
		itemGetRequest.getParams().setHostids(Arrays.asList(hostid));
		List<Integer> itemids = new ArrayList<Integer>();
		for (ItemObject item : items) {
			itemids.add(item.getItemid());
		}
		itemGetRequest.getParams().setItemids(itemids);
		itemGetRequest.getParams().setSortField("key_");

		ItemGetResponse itemGetResponse = zabbixApi.item().get(itemGetRequest);

		Map<Integer, Result> result = new HashMap<Integer, Result>();
		for (Result r : itemGetResponse.getResult()) {
			result.put(r.getItemid(), r);
		}
		return result;
	}

	public Map<Integer, ItemObject> getItems(List<Integer> itemids) throws Throwable {
		ItemGetRequest request = new ItemGetRequest();
		request.getParams().setItemids(itemids);

		ItemGetResponse response = zabbixApi.item().get(request);

		Map<Integer, ItemObject> result = new HashMap<Integer, ItemObject>();
		for (com.zabbix4j.item.ItemGetResponse.Result item : response.getResult()) {
			result.put(item.getItemid(), item);
		}
		return result;
	}

	public Map<Integer, HostObject> searchHostsByName(String... hostNames) throws Throwable {
		try {
			ImmutableMap<String, Map<Integer, HostObject>> all = hostNameCache.getAll(Arrays.asList(hostNames));
			Map<Integer, HostObject> result = new HashMap<Integer, HostObject>();
			for (Map.Entry<String, Map<Integer, HostObject>> entry : all.entrySet()) {
				result.putAll(entry.getValue());
			}
			return result;
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	protected Map<Integer, HostObject> populateHostsByName(String hostName) throws ZabbixApiException {
		Map<Integer, HostObject> result = new HashMap<Integer, HostObject>();

		HostGetRequest request = new HostGetRequest();
		Map<String, String> search = new HashMap<String, String>();
		search.put("name", hostName);
		request.getParams().setSearch(search);

		HostGetResponse response = zabbixApi.host().get(request);
		for (com.zabbix4j.host.HostGetResponse.Result hr : response.getResult()) {
			result.put(hr.getHostid(), hr);
		}
		return result;
	}

	public Map<Integer, List<ItemObject>> searchItemsByKey(Collection<Integer> hostids, String keyName)
	      throws ZabbixApiException {
		ItemGetRequest request = new ItemGetRequest();
		List<Integer> hostidList = new ArrayList<Integer>();
		hostidList.addAll(hostids);
		request.getParams().setHostids(hostidList);
		Map<String, String> search = new HashMap<String, String>();
		search.put("key_", keyName);
		request.getParams().setSearch(search);

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

	public Map<Integer, List<ItemObject>> searchItemsByName(Collection<Integer> hostids, String searchName)
	      throws ZabbixApiException {
		ItemGetRequest request = new ItemGetRequest();
		List<Integer> hostidList = new ArrayList<Integer>();
		hostidList.addAll(hostids);
		request.getParams().setHostids(hostidList);
		Map<String, String> search = new HashMap<String, String>();
		search.put("name", searchName);
		request.getParams().setSearch(search);

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

}
