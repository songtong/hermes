package com.ctrip.hermes.monitor.service;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.ctrip.hermes.monitor.Bootstrap;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.domain.MonitorItem;
import com.ctrip.hermes.monitor.stat.StatResult;
import com.ctrip.hermes.monitor.zabbix.ZabbixApiGateway;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;
import com.zabbix4j.ZabbixApiException;
import com.zabbix4j.history.HistoryObject.HISOTRY_OBJECT_TYPE;
import com.zabbix4j.host.HostObject;
import com.zabbix4j.item.ItemObject;

@Service
public class KafkaMonitor implements IZabbixMonitor {

	private static final Logger logger = LoggerFactory.getLogger(KafkaMonitor.class);

	public static void main(String[] args) throws Throwable {
		ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class);
		KafkaMonitor monitor = context.getBean(KafkaMonitor.class);
		monitor.monitorPastHours(24 * 2, 5);
		context.close();
	}

	@Autowired
	private ESMonitorService service;

	@Autowired
	private ZabbixApiGateway zabbixApi;

	@Autowired
	private MonitorConfig config;

	@Scheduled(cron = "0 5 * * * *")
	public void monitorHourly() throws Throwable {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		Date timeTill = cal.getTime();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		Date timeFrom = cal.getTime();

		monitorKafka(timeFrom, timeTill);
	}

	private void monitorKafka(Date timeFrom, Date timeTill) throws Throwable {
		Map<Integer, HostObject> kafkaHosts = zabbixApi.searchHostsByName(config.getZabbixKafkaBrokerHosts());
		Map<Integer, StatResult> messageInStat = statMessageIn(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> byteInStat = statByteIn(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> byteOutStat = statByteOut(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> failedProduceRequestsStat = statFailedProduceRequests(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> failedFetchRequestsStat = statFailedFetchRequests(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> requestQueueSizeStat = statRequestQueueSize(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> requestRateProduceStat = statRequestRateProduce(timeFrom, timeTill, kafkaHosts);
		Map<Integer, StatResult> requestRateFetchConsumerStat = statRequestRateFetchConsumer(timeFrom, timeTill,
		      kafkaHosts);
		Map<Integer, StatResult> requestRateFetchFollowerStat = statRequestRateFetchFollower(timeFrom, timeTill,
		      kafkaHosts);

		int MINUTE_IN_SECONDS = 60;

		for (Integer hostid : kafkaHosts.keySet()) {
			Map<String, Object> stat = new HashMap<String, Object>();
			stat.put("kafka.messagein.sumbymin", messageInStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.messagein.meanbysec", messageInStat.get(hostid).getMean());
			stat.put("kafka.bytein.sumbymin", byteInStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.bytein.meanbysec", byteInStat.get(hostid).getMean());
			stat.put("kafka.byteout.sumbymin", byteOutStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.byteout.meanbysec", byteOutStat.get(hostid).getMean());
			stat.put("kafka.failedproducerequest.sumbymin", failedProduceRequestsStat.get(hostid).getSum()
			      * MINUTE_IN_SECONDS);
			stat.put("kafka.failedproducerequest.meanbysec", failedProduceRequestsStat.get(hostid).getMean());
			stat.put("kafka.failedfetchrequest.sumbymin", failedFetchRequestsStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.failedfetchrequest.meanbysec", failedFetchRequestsStat.get(hostid).getMean());
			stat.put("kafka.requestqueuesize.meanbysec", requestQueueSizeStat.get(hostid).getMean());
			stat.put("kafka.produce.sumbymin", requestRateProduceStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.produce.meanbysec", requestRateProduceStat.get(hostid).getMean());
			stat.put("kafka.fetchconsumer.sumbymin", requestRateFetchConsumerStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.fetchconsumer.meanbysec", requestRateFetchConsumerStat.get(hostid).getMean());
			stat.put("kafka.fetchfollower.sumbymin", requestRateFetchFollowerStat.get(hostid).getSum() * MINUTE_IN_SECONDS);
			stat.put("kafka.fetchfollower.meanbysec", requestRateFetchFollowerStat.get(hostid).getMean());

			MonitorItem item = new MonitorItem();
			item.setCategory(ZabbixConst.CATEGORY_KAFKA);
			item.setSource(ZabbixConst.SOURCE_ZABBIX);
			item.setStartDate(timeFrom);
			item.setEndDate(timeTill);
			item.setHost(kafkaHosts.get(hostid).getHost());
			item.setGroup(ZabbixConst.GROUP_KAFKA_BROKER);
			item.setValue(stat);

			try {
				IndexResponse response = service.prepareIndex(item);
				logger.info(response.getId());
			} catch (IOException e) {
				logger.warn("Save item failed", e);
			}
		}
	}

	public void monitorPastHours(int hours, int requestIntervalSecond) throws Throwable {
		for (int i = hours - 1; i >= 0; i--) {
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.add(Calendar.HOUR_OF_DAY, -i);
			Date timeTill = cal.getTime();
			cal.add(Calendar.HOUR_OF_DAY, -1);
			Date timeFrom = cal.getTime();

			monitorKafka(timeFrom, timeTill);

			try {
				Thread.sleep(requestIntervalSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
	}

	private Map<Integer, StatResult> statByteIn(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.KAFKA_BYTE_IN_RATE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format("%14s Bytes In(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)", hosts
			      .get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Bytes In(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statByteOut(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(), ZabbixConst.KAFKA_BYTE_OUT_RATE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format("%14s Bytes Out(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)", hosts
			      .get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Bytes Out(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statFailedFetchRequests(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_FAILED_FETCH_REQUESTS);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += validResult.getSum() * 60;
			logger.info(String.format(
			      "%14s Failed Fetch Request(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)", hosts.get(hostid)
			            .getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Failed Fetch Request(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statFailedProduceRequests(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_FAILED_PRODUCE_REQUESTS);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format(
			      "%14s Failed Produce Request(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Failed Produce Request(%s - %s) %,12d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statMessageIn(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_MESSAGE_IN_RATE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format("%14s Message In(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)", hosts
			      .get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Message In(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statRequestQueueSize(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_REQUEST_QUEUE_SIZE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getMean() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += validResult.getMean();
			logger.info(String.format("%14s Request Queue Size(%s - %s) %,9.0f(By Second) ", hosts.get(hostid).getHost(),
			      timeFrom, timeTill, validResult.getMean()));
		}
		logger.info(String.format("%14s Request Queue Size(%s - %s) %,12d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statRequestRateFetchConsumer(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_REQUEST_RATE_FETCHCONSUMER);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format("%14s Fetch Consumer(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Fetch Consumer(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statRequestRateFetchFollower(Date timeFrom, Date timeTill,
	      Map<Integer, HostObject> hosts) throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_REQUEST_RATE_FETCHFOLLOWER);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format("%14s Fetch Follower(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Fetch Follower(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}

	private Map<Integer, StatResult> statRequestRateProduce(Date timeFrom, Date timeTill, Map<Integer, HostObject> hosts)
	      throws ZabbixApiException {
		Map<Integer, List<ItemObject>> ids = zabbixApi.searchItemsByName(hosts.keySet(),
		      ZabbixConst.KAFKA_REQUEST_RATE_PRODUCE);
		Map<Integer, StatResult> result = new HashMap<Integer, StatResult>();
		long totalSum = 0;
		for (Integer hostid : hosts.keySet()) {
			Map<Integer, StatResult> statResults = zabbixApi.getHistoryStat(timeFrom, timeTill, hostid, ids.get(hostid),
			      HISOTRY_OBJECT_TYPE.INTEGER);
			StatResult validResult = new StatResult();
			for (StatResult value : statResults.values()) {
				if (value.getSum() > 0) {
					validResult = value;
					break;
				}
			}
			result.put(hostid, validResult);
			totalSum += (validResult.getSum() * 60);
			logger.info(String.format("%14s Produce(%s - %s) Sum: %,15.0f(By Minute), Mean: %,9.0f(By Second)",
			      hosts.get(hostid).getHost(), timeFrom, timeTill, validResult.getSum() * 60, validResult.getMean()));
		}
		logger.info(String.format("%14s Produce(%s - %s) %,15d ", "Total", timeFrom, timeTill, totalSum));
		return result;
	}
}
