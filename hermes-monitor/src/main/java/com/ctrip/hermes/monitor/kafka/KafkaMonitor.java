package com.ctrip.hermes.monitor.kafka;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.MonitorReport;
import com.ctrip.hermes.metaservice.model.MonitorReportDao;
import com.ctrip.hermes.monitor.zabbix.ZabbixApiUtils;
import com.ctrip.hermes.monitor.zabbix.ZabbixIds;
import com.ctrip.hermes.monitor.zabbix.ZabbixStatUtils;
import com.google.gson.Gson;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;

@Service
public class KafkaMonitor {

	private MonitorReportDao dao = PlexusComponentLocator.lookup(MonitorReportDao.class);

	public static void main(String[] args) throws ZabbixApiException, DalException {
		KafkaMonitor monitor = new KafkaMonitor();
		// moniotor.getTodayKafkaMonitor();
		// monitor.getYesterdayKafkaMonitor();
		monitor.getKafkaMonitorHourly();
	}

	public long getByteInCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long byteCount = ZabbixStatUtils.getHistorySumByMinute(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Byte_In_Rate_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return byteCount;
	}

	public Map<Integer, Long> getByteInCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, Long> result = new HashMap<Integer, Long>();
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getByteInCount(timeFrom, timeTill, hostid);
			result.put(hostid, count);
			totalCount += count;
			System.out.format("%14s Bytes In(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom, timeTill, count);
		}
		System.out.format("%14s Bytes In(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
		return result;
	}

	public long getByteOutCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long byteCount = ZabbixStatUtils.getHistorySumByMinute(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Byte_Out_Rate_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return byteCount;
	}

	public Map<Integer, Long> getByteOutCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, Long> result = new HashMap<Integer, Long>();
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getByteOutCount(timeFrom, timeTill, hostid);
			result.put(hostid, count);
			totalCount += count;
			System.out.format("%14s Bytes Out(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom, timeTill, count);
		}
		System.out.format("%14s Bytes Out(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
		return result;
	}

	public long getFailedFetchRequestsCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long requestCount = ZabbixStatUtils.getHistorySumByMinute(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Failed_Fetch_Requests_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return requestCount;
	}

	public Map<Integer, Long> getFailedFetchRequestsCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, Long> result = new HashMap<Integer, Long>();
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getFailedFetchRequestsCount(timeFrom, timeTill, hostid);
			result.put(hostid, count);
			totalCount += count;
			System.out.format("%14s Failed Fetch Request(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, count);
		}
		System.out.format("%14s Failed Fetch Request(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
		return result;
	}

	public long getFailedProduceRequestsCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long requestCount = ZabbixStatUtils.getHistorySumByMinute(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Failed_Produce_Requests_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return requestCount;
	}

	public Map<Integer, Long> getFailedProduceRequestsCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, Long> result = new HashMap<Integer, Long>();
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getFailedProduceRequestsCount(timeFrom, timeTill, hostid);
			result.put(hostid, count);
			totalCount += count;
			System.out.format("%14s Failed Produce Request(%s - %s) %,12d \n", hosts.get(hostid).getHost(), timeFrom,
			      timeTill, count);
		}
		System.out.format("%14s Failed Produce Request(%s - %s) %,12d \n", "Total", timeFrom, timeTill, totalCount);
		return result;
	}

	public long getMessageInCount(Date timeFrom, Date timeTill, Integer hostid) throws ZabbixApiException {
		long messageCount = ZabbixStatUtils.getHistorySumByMinute(timeFrom, timeTill, hostid,
		      ZabbixIds.Kafka_Message_In_Rate_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
		return messageCount;
	}

	public Map<Integer, Long> getMessageInCount(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, Long> result = new HashMap<Integer, Long>();
		long totalCount = 0;
		for (Integer hostid : hostids) {
			long count = getMessageInCount(timeFrom, timeTill, hostid);
			result.put(hostid, count);
			totalCount += count;
			System.out.format("%14s Message In(%s - %s) %,15d \n", hosts.get(hostid).getHost(), timeFrom, timeTill, count);
		}
		System.out.format("%14s Message In(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalCount);
		return result;
	}

	public void getYesterdayKafkaMonitor() throws ZabbixApiException {
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

	@Scheduled(fixedRate = 3600000)
	public void getKafkaMonitorHourly() throws ZabbixApiException, DalException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
		Date timeFrom = cal.getTime();

		Map<Integer, Long> messageInCount = getMessageInCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, Long> byteInCount = getByteInCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, Long> byteOutCount = getByteOutCount(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, Long> failedProduceRequestsCount = getFailedProduceRequestsCount(timeFrom, timeTill,
		      ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, Long> failedFetchRequestsCount = getFailedFetchRequestsCount(timeFrom, timeTill,
		      ZabbixIds.Kafka_Broker_Hostids);

		Gson gson = new Gson();
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils
		      .getHosts(ZabbixIds.Kafka_Broker_Hostids);
		for (Integer hostid : ZabbixIds.Kafka_Broker_Hostids) {
			Map<String, Object> stat = new HashMap<String, Object>();
			stat.put("MessageInCount", messageInCount.get(hostid));
			stat.put("ByteInCount", byteInCount.get(hostid));
			stat.put("ByteOutCount", byteOutCount.get(hostid));
			stat.put("FailedProduceRequestCount", failedProduceRequestsCount.get(hostid));
			stat.put("FailedFetchRequestCount", failedFetchRequestsCount.get(hostid));
			String json = gson.toJson(stat);
			MonitorReport report = new MonitorReport();
			report.setCategory("Kafka");
			report.setSource("Zabbix");
			report.setStart(timeFrom);
			report.setEnd(timeTill);
			report.setHost(hosts.get(hostid).getHost());
			report.setValue(json);
			dao.insert(report);
		}
	}
}
