package com.ctrip.hermes.portal.service.dashboard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessageQueueDao;
import com.ctrip.hermes.metaservice.service.DefaultPortalMetaService;
import com.ctrip.hermes.metaservice.service.PartitionService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.resource.view.BrokerQPSBriefView;
import com.ctrip.hermes.portal.resource.view.BrokerQPSDetailView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;
import com.ctrip.hermes.portal.service.elastic.PortalElasticClient;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named(type = DashboardService.class)
public class DefaultDashboardService implements DashboardService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultDashboardService.class);

	private static final int CONSUMER_BACKLOG_CALCULATE_THREAD_COUNT = 10;

	private static final int CONSUMER_BACKLOG_CALCULATE_AWAITTIME_MINUTE = 1;

	@Inject
	private MessageQueueDao m_dao;

	@Inject
	private DefaultPortalMetaService m_metaService;

	@Inject
	private TopicService m_topicService;
	
	@Inject
	private PartitionService m_partitionService;
	
	@Inject
	private PortalElasticClient m_elasticClient;

	@Inject
	private ClientEnvironment m_env;

	private List<String> m_latestBroker = new ArrayList<String>();

	private Set<String> m_latestClients = new HashSet<String>();

	private Map<String, Date> m_latestProduced = new HashMap<>();

	// key: topic, value: ips
	private Map<String, Set<String>> m_topic2producers = new HashMap<>();

	// key: topic, vlaue.key: consumerName, value.value: ips>
	private Map<String, Map<String, Set<String>>> m_topic2consumers = new HashMap<>();

	// key: producer ip, value: topics
	private Map<String, Set<String>> m_producer2topics = new HashMap<>();

	// key: consumer ip, value.key: consumerName, value.value: topics
	private Map<String, Map<String, Set<String>>> m_consumer2topics = new HashMap<>();

	private long m_timeStamp = -1;

	private Map<Tpg, String> m_consumerLeases = new HashMap<>();

	private Map<Pair<String, Integer>, String> m_brokerLeases = new HashMap<>();

	private ExecutorService m_es = Executors.newFixedThreadPool(CONSUMER_BACKLOG_CALCULATE_THREAD_COUNT);

	private Cache<Pair<String, String>, List<DelayDetail>> cachedConsumerBacklogs = CacheBuilder.newBuilder()
			.maximumSize(20)
			.expireAfterWrite(PortalConstants.CONSUMER_BACKLOG_EXPIRED_TIME_MIllIS, TimeUnit.MILLISECONDS).build();

	@Override
	public Date getLatestProduced(String topic) {
		Date date = m_latestProduced.get(topic);
		return date == null ? new Date(0) : date;
	}

	@Override
	public Map<String, Set<String>> getTopic2ProducerIPs() {
		return m_topic2producers;
	}

	@Override
	public Map<String, Map<String, Set<String>>> getTopic2ConsumerIPs() {
		return m_topic2consumers;
	}

	@Override
	public Map<String, Map<String, Set<String>>> getConsumerIP2Topics() {
		return m_consumer2topics;
	}

	@Override
	public Map<String, Set<String>> getProducerIP2Topics() {
		return m_producer2topics;
	}

	@Override
	public List<String> getLatestBrokers() {
		return m_latestBroker;
	}

	private Meta loadMeta() {
		try {
			String url = String.format("http://%s:%s/%s", m_env.getMetaServerDomainName(),
					m_env.getGlobalConfig().getProperty("meta.port", "80").trim(), "meta");
			HttpResponse response = Request.Get(url).execute().returnResponse();
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == HttpStatus.SC_OK) {
				String responseContent = EntityUtils.toString(response.getEntity());
				return JSON.parseObject(responseContent, Meta.class);
			}
			log.warn("Loading meta from meta-servers, status code is {}", statusCode);
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Load meta from meta-servers faied.", e);
			}
		}
		return m_metaService.getMetaEntity();
	}

	private String getMetaserverStatusString() {
		String metaServer = null;
		String url = String.format("http://%s:%s/%s", m_env.getMetaServerDomainName(),
				m_env.getGlobalConfig().getProperty("meta.port", "80").trim(), "metaserver/status");
		try {
			HttpResponse response = Request.Get(url).execute().returnResponse();
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == HttpStatus.SC_OK) {
				metaServer = EntityUtils.toString(response.getEntity());
			}
		} catch (IOException e) {
			if (log.isDebugEnabled()) {
				log.debug("Load metaserver/status from meta-servers faied.", e);
			}
		}
		return metaServer;
	}

	private void parseLeases(String metaServer) {
		Map<Tpg, String> temp_consumerLeases = new HashMap<>();
		Map<Pair<String, Integer>, String> temp_brokerLeases = new HashMap<>();
		JSONObject metaServerJsonObject = JSON.parseObject(metaServer);

		JSONObject consumerLeases = metaServerJsonObject.getJSONObject("consumerLeases");
		for (Entry<String, Object> entry : consumerLeases.entrySet()) {
			JSONObject lease = (JSONObject) entry.getValue();
			String ip = ((JSONObject) lease.entrySet().iterator().next().getValue()).getString("ip");
			String[] splitKey = entry.getKey().split(",|=|]");
			if (splitKey.length != 6) {
				log.warn("Parse comsumer lease for {} failed.", entry.getKey());
				continue;
			}
			String topic = splitKey[1].trim();
			int partition = new Integer(splitKey[3].trim());
			String group = splitKey[5].trim();
			Tpg tpg = new Tpg(topic, partition, group);
			temp_consumerLeases.put(tpg, ip);
		}
		m_consumerLeases = temp_consumerLeases;

		JSONObject brokerLeases = metaServerJsonObject.getJSONObject("brokerLeases");
		for (Entry<String, Object> entry : brokerLeases.entrySet()) {
			JSONObject lease = (JSONObject) entry.getValue();
			String ip = ((JSONObject) lease.entrySet().iterator().next().getValue()).getString("ip");
			String[] splitKey = entry.getKey().split(",|=|]");
			if (splitKey.length != 4) {
				log.warn("Parse broker lease for {} failed.", entry.getKey());
				continue;
			}
			String topic = splitKey[1].trim();
			int partition = new Integer(splitKey[3].trim());
			temp_brokerLeases.put(new Pair<String, Integer>(topic, partition), ip);
		}
		m_brokerLeases = temp_brokerLeases;

	}

	private void updateLatestBroker() {
		List<String> list = new ArrayList<String>();
		Meta meta = loadMeta();
		if (meta != null) {
			for (Entry<String, Endpoint> entry : meta.getEndpoints().entrySet()) {
				if (Endpoint.BROKER.equals(entry.getValue().getType())) {
					String host = entry.getValue().getHost();
					host = host.equals("localhost") || host.equals("127.0.0.1") ? PortalConstants.LOCALHOST : host;
					list.add(host);
				}
			}
		} else {
			log.warn("Can not load meta from either meta-servers or db.");
		}
		m_latestBroker = list;
	}

	@Override
	public List<DelayDetail> getDelayDetailForConsumer(String topic, String consumer) {
		List<DelayDetail> consumerDelays = cachedConsumerBacklogs
				.getIfPresent(new Pair<String, String>(topic, consumer));
		if (consumerDelays != null) {
			return consumerDelays;
		}
		consumerDelays = new ArrayList<>();
		if (System.currentTimeMillis() - m_timeStamp > PortalConstants.CONSUMER_BACKLOG_EXPIRED_TIME_MIllIS) {
			synchronized (this) {
				if (System.currentTimeMillis() - m_timeStamp > PortalConstants.CONSUMER_BACKLOG_EXPIRED_TIME_MIllIS) {
					parseLeases(getMetaserverStatusString());
					m_timeStamp = System.currentTimeMillis();
				}
			}
		}

		Topic t = m_topicService.findTopicByName(topic);
		CountDownLatch latch = new CountDownLatch(t.getPartitions().size());
		try {
			ConsumerGroup c = t.findConsumerGroup(consumer);
			if (Storage.MYSQL.equals(t.getStorageType())) {
				for (Partition p : t.getPartitions()) {
					m_es.execute(new ConsumerBacklogCalculateTask(topic, c, p.getId(), latch, m_dao, consumerDelays));
				}
				latch.await(CONSUMER_BACKLOG_CALCULATE_AWAITTIME_MINUTE, TimeUnit.MINUTES);
				for (DelayDetail delay : consumerDelays) {
					delay.setCurrentConsumerIp(m_consumerLeases.get(new Tpg(topic, delay.getPartitionId(), consumer)));
					delay.setCurrentBrokerIp(
							m_brokerLeases.get(new Pair<String, Integer>(topic, delay.getPartitionId())));
				}
			}
		} catch (InterruptedException e) {
			log.warn("Generate consumer: {} backlog info failed.", consumer);
		}
		Collections.sort(consumerDelays, new Comparator<DelayDetail>() {

			@Override
			public int compare(DelayDetail delay1, DelayDetail delay2) {
				return delay1.getPartitionId() - delay2.getPartitionId();
			}
		});
		cachedConsumerBacklogs.put(new Pair<String, String>(topic, consumer), consumerDelays);
		return consumerDelays;
	}

	private void updateLatestProduced() {
		Map<String, Date> m = new HashMap<String, Date>();
		for (Entry<String, Topic> entry : m_topicService.getTopics().entrySet()) {
			Topic topic = entry.getValue();
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				String topicName = topic.getName();
				Date current = m_latestProduced.get(topicName) == null ? new Date(0) : m_latestProduced.get(topicName);
				Date latest = new Date(0);
				for (Partition partition : m_partitionService.findPartitionsByTopic(topic.getId())) {
					try {
						MessagePriority msgPriority = m_dao.getLatestProduced(topicName, partition.getId(),
								PortalConstants.PRIORITY_TRUE);
						Date datePriority = msgPriority == null ? latest : msgPriority.getCreationDate();
						MessagePriority msgNonPriority = m_dao.getLatestProduced(topicName, partition.getId(),
								PortalConstants.PRIORITY_FALSE);

						Date dateNonPriority = msgNonPriority == null ? latest : msgNonPriority.getCreationDate();
						latest = datePriority.after(dateNonPriority) ? datePriority : dateNonPriority;
					} catch (DalException e) {
						log.warn("Find latest produced failed. {}:{}", topicName, partition.getId());
						continue;
					}
					current = latest.after(current) ? latest : current;
				}
				m.put(topicName, current);
			}
		}
		m_latestProduced = m;
	}

	private void updateProducerTopicRelationship() {
		Map<String, Set<String>> topic2producers = new HashMap<String, Set<String>>();
		for (String topic : m_topicService.getTopics().keySet()) {
			topic2producers.put(topic, new HashSet<String>(m_elasticClient.getLastWeekProducers(topic)));
		}
		m_topic2producers = topic2producers;

		Map<String, Set<String>> producer2topics = new HashMap<String, Set<String>>();
		for (Entry<String, Set<String>> entry : topic2producers.entrySet()) {
			String topicName = entry.getKey();
			for (String ip : entry.getValue()) {
				Set<String> topics = producer2topics.get(ip);
				if (topics == null) {
					producer2topics.put(ip, topics = new HashSet<String>());
				}
				topics.add(topicName);
			}
		}
		m_producer2topics = producer2topics;
	}

	private void updateConsumerTopicRelationship() {
		Map<String, Map<String, Set<String>>> topic2consumers = new HashMap<>();
		for (Entry<String, Topic> entry : m_topicService.getTopics().entrySet()) {
			String topic = entry.getKey();
			for (ConsumerGroup c : entry.getValue().getConsumerGroups()) {
				String consumer = c.getName();
				if (!topic2consumers.containsKey(topic)) {
					topic2consumers.put(topic, new HashMap<String, Set<String>>());
				}
				HashSet<String> set = new HashSet<String>(m_elasticClient.getLastWeekConsumers(topic, consumer));
				topic2consumers.get(topic).put(consumer, set);
			}
		}
		m_topic2consumers = topic2consumers;

		Map<String, Map<String, Set<String>>> consumer2topics = new HashMap<String, Map<String, Set<String>>>();
		for (Entry<String, Map<String, Set<String>>> entry : topic2consumers.entrySet()) {
			String topicName = entry.getKey();
			for (Entry<String, Set<String>> ips : entry.getValue().entrySet()) {
				String groupName = ips.getKey();
				for (String ip : ips.getValue()) {
					Map<String, Set<String>> topics = consumer2topics.get(ip);
					if (topics == null) {
						consumer2topics.put(ip, topics = new HashMap<String, Set<String>>());
					}
					Set<String> set = topics.get(groupName);
					if (set == null) {
						topics.put(groupName, set = new HashSet<String>());
					}
					set.add(topicName);
				}
			}
		}
		m_consumer2topics = consumer2topics;
	}

	private void updateLatestClients() {
		Set<String> set = new HashSet<String>(m_consumer2topics.keySet());
		set.addAll(m_producer2topics.keySet());
		m_latestClients = set;
	}

	@Override
	public void initialize() throws InitializationException {
		updateLatestBroker();

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_MYSQL_UPDATE_TASK", true))
				.scheduleWithFixedDelay(new Runnable() {
					@Override
					public void run() {
						try {
							updateLatestProduced();
							updateLatestBroker();
						} catch (Throwable e) {
							log.error("Update mysql monitor information failed.", e);
						}
					}
				}, 0, 1, TimeUnit.MINUTES);

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_ELASTIC_UPDATE_TASK", true))
				.scheduleWithFixedDelay(new Runnable() {
					@Override
					public void run() {
						try {
							updateProducerTopicRelationship();
							updateConsumerTopicRelationship();
							updateLatestClients();
						} catch (Throwable e) {
							log.error("Update elastic monitor information failed.", e);
						}
					}
				}, 0, 30, TimeUnit.MINUTES);
	}

	@Override
	public List<String> getRelatedClients(String part) {
		List<String> list = new ArrayList<String>();
		for (String client : m_latestClients) {
			if (client.contains(part)) {
				list.add(client);
			}
		}
		Collections.sort(list);
		return list;
	}

	@Override
	public List<Pair<String, Date>> getTopOutdateTopic(int top) {
		List<Pair<String, Date>> list = new ArrayList<Pair<String, Date>>();

		for (Entry<String, Date> entry : m_latestProduced.entrySet()) {
			list.add(new Pair<String, Date>(entry.getKey(), entry.getValue()));
		}

		Collections.sort(list, new Comparator<Pair<String, Date>>() {
			@Override
			public int compare(Pair<String, Date> o1, Pair<String, Date> o2) {
				long t1 = o1.getValue().getTime();
				long t2 = o2.getValue().getTime();
				if (t1 == 0 && t2 == 0) {
					return o1.getKey().compareTo(o2.getKey());
				} else if (t1 == 0 || t2 == 0) {
					return t1 == 0 ? 1 : -1;
				}
				return o1.getValue().compareTo(o2.getValue());
			}
		});
		top = top > list.size() ? list.size() : top;
		return list.subList(0, top > 0 ? top : 0);
	}

	private Map<String, Integer> normalizeBrokerQPSMap(Map<String, Integer> map) {
		for (String broker : m_latestBroker) {
			if (!map.containsKey(broker)) {
				map.put(broker, 0);
			}
		}
		return map;
	}

	@Override
	public List<BrokerQPSBriefView> getBrokerReceivedQPS() {
		return BrokerQPSBriefView.convertFromMap(normalizeBrokerQPSMap(m_elasticClient.getBrokerReceived()));
	}

	@Override
	public List<BrokerQPSBriefView> getBrokerDeliveredQPS() {
		return BrokerQPSBriefView.convertFromMap(normalizeBrokerQPSMap(m_elasticClient.getBrokerDelivered()));
	}

	@Override
	public BrokerQPSDetailView getBrokerReceivedDetailQPS(String brokerIp) {
		return new BrokerQPSDetailView(brokerIp, m_elasticClient.getBrokerTopicReceived(brokerIp, 50));
	}

	@Override
	public BrokerQPSDetailView getBrokerDeliveredDetailQPS(String brokerIp) {
		return new BrokerQPSDetailView(brokerIp, m_elasticClient.getBrokerTopicDelivered(brokerIp, 50));
	}

}
