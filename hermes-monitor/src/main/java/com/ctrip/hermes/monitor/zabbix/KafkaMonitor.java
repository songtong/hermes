package com.ctrip.hermes.monitor.zabbix;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.zabbix4j.ZabbixApi;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryGetRequest;
import com.zabbix4j.history.HistoryGetResponse;
import com.zabbix4j.history.HistoryObject;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;

public class KafkaMonitor {

	public static void main(String[] args) throws ZabbixApiException {
		// getTodayKafkaMonitor();
		getYesterdayKafkaMonitor();
	}

	public static void getTodayKafkaMonitor() throws ZabbixApiException {
		Calendar cal = Calendar.getInstance();
		Date timeTill = cal.getTime();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeFrom = cal.getTime();

		getMessageInCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getByteInCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getByteOutCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getFailedProduceRequestsCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getFailedFetchRequestsCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
	}

	public static void getYesterdayKafkaMonitor() throws ZabbixApiException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) - 1);
		Date timeFrom = cal.getTime();

		getMessageInCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getByteInCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getByteOutCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getFailedProduceRequestsCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		getFailedFetchRequestsCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
	}

	public static void getMessageInCount(Date timeFrom, Date timeTill, List<Integer> hostids) throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getMessageInCount(timeFrom, timeTill, hostid);
			totalCount += count;
			System.out.format("%14s Message In(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom, timeTill, count);
		}
		System.out.format("%14s Message In(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
	}

	public static void getByteInCount(Date timeFrom, Date timeTill, List<Integer> hostids) throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getByteInCount(timeFrom, timeTill, hostid);
			totalCount += count;
			System.out.format("%14s Bytes In(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom, timeTill, count);
		}
		System.out.format("%14s Bytes In(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
	}

	public static void getByteOutCount(Date timeFrom, Date timeTill, List<Integer> hostids) throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getByteOutCount(timeFrom, timeTill, hostid);
			totalCount += count;
			System.out.format("%14s Bytes Out(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom, timeTill, count);
		}
		System.out.format("%14s Bytes Out(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
	}

	public static void getFailedFetchRequestsCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getFailedFetchRequestsCount(timeFrom, timeTill, hostid);
			totalCount += count;
			System.out.format("%14s Failed Fetch Request(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, count);
		}
		System.out.format("%14s Failed Fetch Request(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
	}

	public static void getFailedProduceRequestsCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getFailedProduceRequestsCount(timeFrom, timeTill, hostid);
			totalCount += count;
			System.out.format("%14s Failed Produce Request(%s - %s) %,12d \n", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, count);
		}
		System.out.format("%14s Failed Produce Request(%s - %s) %,12d \n", "Total", timeFrom, timeTill, totalCount);
	}

	public static long getMessageInCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long messageCount = getHistorySumByMinutes(timeFrom, timeTill, hostid, ZabbixIds.Kafka_Message_In_Rate_Itemids,
		      HISOTRY_OBJECT_TYPE.INTEGER);
		return messageCount;
	}

	public static long getByteInCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long byteCount = getHistorySumByMinutes(timeFrom, timeTill, hostid, ZabbixIds.Kafka_Byte_In_Rate_Itemids,
		      HISOTRY_OBJECT_TYPE.INTEGER);
		return byteCount;
	}

	public static long getByteOutCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long byteCount = getHistorySumByMinutes(timeFrom, timeTill, hostid, ZabbixIds.Kafka_Byte_Out_Rate_Itemids,
		      HISOTRY_OBJECT_TYPE.INTEGER);
		return byteCount;
	}

	public static long getFailedFetchRequestsCount(Date timeFrom, Date timeTill, Integer hostid)
	      throws ZabbixApiException {
		long requestCount = getHistorySumByMinutes(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Failed_Fetch_Requests_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return requestCount;
	}

	public static long getFailedProduceRequestsCount(Date timeFrom, Date timeTill, Integer hostid)
	      throws ZabbixApiException {
		long requestCount = getHistorySumByMinutes(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Failed_Produce_Requests_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return requestCount;
	}

	private static long getHistorySumByMinutes(Date timeFrom, Date timeTill, Integer hostid, List<Integer> itemids,
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

		long byteCount = 0;
		for (HistoryObject result : hisotryGetResponse.getResult()) {
			long messagePerSec = Integer.parseInt(result.getValue());
			byteCount += (messagePerSec * 60);
		}
		return byteCount;
	}
}
