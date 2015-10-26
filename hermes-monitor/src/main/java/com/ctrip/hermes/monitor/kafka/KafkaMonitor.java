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
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;
import com.ctrip.hermes.monitor.zabbix.ZabbixStatUtils;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemObject;

@Service
public class KafkaMonitor {

	public static void main(String[] args) throws ZabbixApiException, DalException {
		KafkaMonitor monitor = new KafkaMonitor();
		monitor.service = new MonitorReportService();
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

		Map<Integer, HostObject> kafkaHosts = ZabbixApiUtils.searchHosts(ZabbixConst.GROUP_NAME_KAFKA);
		statMessageIn(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		statByteIn(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		statByteOut(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		statFailedProduceRequests(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		statFailedFetchRequests(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		statRequestQueueSize(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		statRequestRateProduce(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		statRequestRateFetchConsumer(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		statRequestRateFetchFollower(timeFrom, timeTill,
				kafkaHosts);
	}

	@Scheduled(fixedRate = 3600000)
	public void monitorHourly() throws ZabbixApiException, DalException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
		Date timeFrom = cal.getTime();

		Map<Integer, HostObject> kafkaHosts = ZabbixApiUtils.searchHosts(ZabbixConst.GROUP_NAME_KAFKA);
		Map<Integer, StatResult> messageInStat = statMessageIn(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> byteInStat = statByteIn(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> byteOutStat = statByteOut(timeFrom, timeTill, kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> failedProduceRequestsStat = statFailedProduceRequests(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> failedFetchRequestsStat = statFailedFetchRequests(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> requestQueueSizeStat = statRequestQueueSize(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> requestRateProduceStat = statRequestRateProduce(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> requestRateFetchConsumerStat = statRequestRateFetchConsumer(timeFrom, timeTill,
				kafkaHosts);
		System.out.format("******************************************************************\n");
		Map<Integer, StatResult> requestRateFetchFollowerStat = statRequestRateFetchFollower(timeFrom, timeTill,
				kafkaHosts);

		int MINUTE_IN_SECONDS = 60;
		
		for (Integer hostid : kafkaHosts.keySet()) {
			Map<String, Number> stat = new HashMap<String, Number>();
			stat.put("MessageInSumByMin", messageInStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("MessageInMeanBySec", messageInStat.get(hostid).getMean());
			stat.put("ByteInSumByMin", byteInStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("ByteInMeanBySec", byteInStat.get(hostid).getMean());
			stat.put("ByteOutSumByMin", byteOutStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("ByteOutMeanBySec", byteOutStat.get(hostid).getMean());
			stat.put("FailedProduceRequestSumByMin", failedProduceRequestsStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("FailedProduceRequestMeanBySec", failedProduceRequestsStat.get(hostid).getMean());
			stat.put("FailedFetchRequestSumByMin", failedFetchRequestsStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("RequestQueueSizeMeanBySec", requestQueueSizeStat.get(hostid).getMean());
			stat.put("ProduceSumByMin", requestRateProduceStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("ProduceMeanBySec", requestRateProduceStat.get(hostid).getMean());
			stat.put("FetchConsumerSumByMin", requestRateFetchConsumerStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("FetchConsumerMeanBySec", requestRateFetchConsumerStat.get(hostid).getMean());
			stat.put("FetchFollowerSumByMin", requestRateFetchFollowerStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("FetchFollowerMeanBySec", requestRateFetchFollowerStat.get(hostid).getMean());
			String json = JSON.toJSONString(stat);
			MonitorReport report = new MonitorReport();
			report.setCategory("Kafka");
			report.setSource("Zabbix");
			report.setStart(timeFrom);
			report.setEnd(timeTill);
			report.setHost(kafkaHosts.get(hostid).getHost());
			report.setValue(json);

			service.insertOrUpdate(report);
		}
	}

	private Map<Integer, StatResult> statRequestRateFetchFollower(Date timeFrom, Date timeTill,
			Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_REQUEST_RATE_FETCHFOLLOWER);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Fetch Follower(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Fetch Follower(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
   }

	private Map<Integer, StatResult> statRequestRateFetchConsumer(Date timeFrom, Date timeTill,
			Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_REQUEST_RATE_FETCHCONSUMER);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
					ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Fetch Consumer(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Fetch Consumer(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
   }

	private Map<Integer, StatResult> statRequestRateProduce(Date timeFrom, Date timeTill,
			Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_REQUEST_RATE_PRODUCE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			System.out.format("%14s Produce(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)\n",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean());
		}
		System.out.format("%14s Produce(%s - %s) %,15d \n", "Total", timeFrom, timeTill, totalSum);
		return result;
   }

	private Map<Integer, StatResult> statByteIn(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_BYTE_IN_RATE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
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

	private Map<Integer, StatResult> statByteOut(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_BYTE_OUT_RATE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
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

	private Map<Integer, StatResult> statFailedFetchRequests(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_FAILED_FETCH_REQUESTS);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
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

	private Map<Integer, StatResult> statFailedProduceRequests(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_FAILED_PRODUCE_REQUESTS);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
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

	private Map<Integer, StatResult> statMessageIn(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_MESSAGE_IN_RATE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
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

	private Map<Integer, StatResult> statRequestQueueSize(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = ZabbixApiUtils.searchItems(hosts.keySet(), ZabbixConst.KAFKA_REQUEST_QUEUE_SIZE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = ZabbixStatUtils.getHistoryStat(timeFrom, timeTill, hostid,
			      ids.get(hostid), HISOTRY_OBJECT_TYPE.INTEGER);
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
