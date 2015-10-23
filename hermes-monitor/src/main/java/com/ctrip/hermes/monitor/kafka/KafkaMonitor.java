package com.ctrip.hermes.monitor.kafka;

import java.util.Calendar;
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
import com.ctrip.hermes.monitor.zabbix.ZabbixIds;
import com.ctrip.hermes.monitor.zabbix.ZabbixStatUtils;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;

@Service
public class KafkaMonitor {

	public static void main(String[] args) throws ZabbixApiException, DalException {
		KafkaMonitor monitor = new KafkaMonitor();
		monitor.service = new MonitorReportService();
		// moniotor.getTodayKafkaMonitor();
		// monitor.getYesterdayKafkaMonitor();
		monitor.monitorHourly();
	}

	@Autowired
	private MonitorReportService service;

	public void monitorDaily() throws ZabbixApiException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) - 1);
		Date timeFrom = cal.getTime();

		statMessageIn(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		statByteIn(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		statByteOut(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		statFailedProduceRequests(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		statFailedFetchRequests(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		statRequestQueueSize(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
	}

	@Scheduled(fixedRate = 3600000)
	public void monitorHourly() throws ZabbixApiException, DalException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
		Date timeFrom = cal.getTime();

		Map<Integer, StatResult> messageInStat = statMessageIn(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> byteInStat = statByteIn(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> byteOutStat = statByteOut(timeFrom, timeTill, ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> failedProduceRequestsStat = statFailedProduceRequests(timeFrom, timeTill,
		      ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> failedFetchRequestsStat = statFailedFetchRequests(timeFrom, timeTill,
		      ZabbixIds.Kafka_Broker_Hostids);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> requestQueueSizeStat = statRequestQueueSize(timeFrom, timeTill,
		      ZabbixIds.Kafka_Broker_Hostids);

		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils
		      .getHosts(ZabbixIds.Kafka_Broker_Hostids);
		for (Integer hostid : ZabbixIds.Kafka_Broker_Hostids) {
			Map<String, Number> stat = new HashMap<String, Number>();
			stat.put("MessageInSumByMin", messageInStat.get(hostid).getSum() * 60);
			stat.put("MessageInMeanBySec", messageInStat.get(hostid).getMean());
			stat.put("ByteInSumByMin", byteInStat.get(hostid).getSum() * 60);
			stat.put("ByteInMeanBySec", byteInStat.get(hostid).getMean());
			stat.put("ByteOutSumByMin", byteOutStat.get(hostid).getSum() * 60);
			stat.put("ByteOutMeanBySec", byteOutStat.get(hostid).getMean());
			stat.put("FailedProduceRequestSumByMin", failedProduceRequestsStat.get(hostid).getSum() * 60);
			stat.put("FailedProduceRequestMeanBySec", failedProduceRequestsStat.get(hostid).getMean());
			stat.put("FailedFetchRequestSumByMin", failedFetchRequestsStat.get(hostid).getSum() * 60);
			stat.put("RequestQueueSizeMeanBySec", requestQueueSizeStat.get(hostid).getMean());
			String json = JSON.toJSONString(stat);
			MonitorReport report = new MonitorReport();
			report.setCategory("Kafka");
			report.setSource("Zabbix");
			report.setStart(timeFrom);
			report.setEnd(timeTill);
			report.setHost(hosts.get(hostid).getHost());
			report.setValue(json);

			service.insertOrUpdate(report);
		}
	}

	public Map<Integer, StatResult> statByteIn(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hostids) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ZabbixIds.Kafka_Byte_In_Rate_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Bytes In(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Bytes In(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
	}

	public Map<Integer, StatResult> statByteOut(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hostids) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ZabbixIds.Kafka_Byte_Out_Rate_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Bytes Out(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Bytes Out(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
	}

	public Map<Integer, StatResult> statFailedFetchRequests(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hostids) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ZabbixIds.Kafka_Failed_Fetch_Requests_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += validResult.getSum() * 60;
			System.out.format("%14s Failed Fetch Request(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Failed Fetch Request(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
	}

	public Map<Integer, StatResult> statFailedProduceRequests(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hostids) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ZabbixIds.Kafka_Failed_Produce_Requests_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Failed Produce Request(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Failed Produce Request(%s - %s) %,12d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
	}

	public Map<Integer, StatResult> statMessageIn(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hostids) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ZabbixIds.Kafka_Message_In_Rate_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Message In(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Message In(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
	}

	public Map<Integer, StatResult> statRequestQueueSize(Date timeFrom, Date timeTill, List<Integer> hostids)
	      throws ZabbixApiException {
		Map<Integer, com.zabbix4j.host.HostGetResponse.Result> hosts = ZabbixApiUtils.getHosts(hostids);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hostids) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ZabbixIds.Kafka_Request_Queue_Size_Itemids, HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getMean() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += validResult.getMean();
			System.out.format("%14s Request Queue Size(%s - %s) %,9.0f(By Second) \n", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean());
		}
		System.out.format("%14s Request Queue Size(%s - %s) %,12d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
	}
}
