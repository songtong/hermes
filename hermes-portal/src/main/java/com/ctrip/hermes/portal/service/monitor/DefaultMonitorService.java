package com.ctrip.hermes.portal.service.monitor;

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
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.dal.HermesPortalDao;
import com.ctrip.hermes.portal.dal.MessagePriority;
import com.ctrip.hermes.portal.dal.OffsetMessage;
import com.ctrip.hermes.portal.resource.view.BrokerQPSBriefView;
import com.ctrip.hermes.portal.resource.view.BrokerQPSDetailView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;
import com.ctrip.hermes.portal.service.elastic.ElasticClient;

@Named(type = MonitorService.class)
public class DefaultMonitorService implements MonitorService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMonitorService.class);

	@Inject
	private HermesPortalDao m_dao;

	@Inject
	private PortalMetaService m_metaService;

	@Inject
	private ElasticClient m_elasticClient;

	@Inject
	private ClientEnvironment m_env;

	private List<String> m_latestBroker = new ArrayList<String>();

	private Set<String> m_latestClients = new HashSet<String>();

	private List<TopicDelayDetailView> m_topDelays = new ArrayList<TopicDelayDetailView>();

	private Map<String, TopicDelayDetailView> m_delays = new HashMap<>();

	// key: topic, value: latest produced date
	private Map<String, Date> m_latestProduced = new HashMap<>();

	// key: topic, value: ips
	private Map<String, Set<String>> m_topic2producers = new HashMap<>();

	// key: topic, vlaue.key: consumerName, value.value: ips>
	private Map<String, Map<String, Set<String>>> m_topic2consumers = new HashMap<>();

	// key: producer ip, value: topics
	private Map<String, Set<String>> m_producer2topics = new HashMap<>();

	// key: consumer ip, value.key: consumerName, value.value: topics
	private Map<String, Map<String, Set<String>>> m_consumer2topics = new HashMap<>();

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
		return m_metaService.getMeta();
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
	public Long getDelay(String topic) {
		long delay = 0;
		TopicDelayDetailView view = m_delays.get(topic);
		if (view == null) {
			log.warn("Delay information of {} not found.", topic);
		} else {
			delay = view.getTotalDelay();
		}
		return delay;
	}

	@Override
	public Long getDelay(String topic, String groupName) {
		long delay = 0;
		TopicDelayDetailView view = m_delays.get(topic);
		if (view == null) {
			log.warn("Delay information of {} not found.", topic);
		} else {
			List<DelayDetail> details = view.getDetails().get(groupName);
			if (details == null) {
				log.warn("Delay information of {}:{} not found.", topic, groupName);
			} else {
				for (DelayDetail detail : details) {
					delay += detail.getDelay();
				}
			}
		}
		return delay;
	}

	@Override
	public TopicDelayDetailView getTopicDelayDetail(String topic) {
		return m_delays.get(topic);
	}

	@Override
	public List<DelayDetail> getDelayDetailForConsumer(String topic, String consumer) {
		List<DelayDetail> details = null;
		TopicDelayDetailView view = m_delays.get(topic);
		if (view == null) {
			log.warn("Delay information of {} not found.", topic);
		} else {
			details = view.getDetails().get(consumer);
			if (details == null) {
				log.warn("Delay information of {}:{} not found.", topic, consumer);
			}
		}
		return details;
	}

	private void updateLatestProduced() {
		Map<String, Date> m = new HashMap<String, Date>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			Topic topic = entry.getValue();
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				String topicName = topic.getName();
				Date current = m_latestProduced.get(topicName) == null ? new Date(0) : m_latestProduced.get(topicName);
				Date latest = new Date(0);
				for (Partition partition : m_metaService.findPartitionsByTopic(topicName)) {
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
		for (String topic : m_metaService.getTopics().keySet()) {
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
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
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

	private void updateTopDelays() {
		Map<String, TopicDelayDetailView> delayMap = new HashMap<String, TopicDelayDetailView>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			Topic t = entry.getValue();
			if (Storage.MYSQL.equals(t.getStorageType())) {
				TopicDelayDetailView topicDelayView = new TopicDelayDetailView(t.getName());
				delayMap.put(t.getName(), topicDelayView);
				for (Partition p : t.getPartitions()) {
					try {
						MessagePriority msgPriority = m_dao.getLatestProduced(t.getName(), p.getId(),
								PortalConstants.PRIORITY_TRUE);
						MessagePriority msgNonPriority = m_dao.getLatestProduced(t.getName(), p.getId(),
								PortalConstants.PRIORITY_FALSE);
						long priorityMsgId = msgPriority == null ? 0 : msgPriority.getId();
						long nonPriorityMsgId = msgNonPriority == null ? 0 : msgNonPriority.getId();
						Map<Integer, Pair<OffsetMessage, OffsetMessage>> offsetMsgMap = m_dao
								.getLatestConsumed(t.getName(), p.getId());
						for (ConsumerGroup c : t.getConsumerGroups()) {
							Pair<OffsetMessage, OffsetMessage> offsets = offsetMsgMap.get(c.getId());
							long priorityMsgOffset = offsets == null ? 0 : offsets.getKey().getOffset();
							long nonPriorityMsgOffset = offsets == null ? 0 : offsets.getValue().getOffset();
							long delay = (priorityMsgId + nonPriorityMsgId)
									- (priorityMsgOffset + nonPriorityMsgOffset);
							MessagePriority lastConsumedPriorityMsg = m_dao.getMsgById(t.getName(), p.getId(),
									PortalConstants.PRIORITY_TRUE, priorityMsgOffset);
							MessagePriority lastConsumedNonPriorityMsg = m_dao.getMsgById(t.getName(), p.getId(),
									PortalConstants.PRIORITY_TRUE, nonPriorityMsgOffset);

							DelayDetail delayDetail = new DelayDetail(c.getName(), p.getId());
							delayDetail.setDelay(delay);
							delayDetail.setPriorityMsgId(priorityMsgId);
							delayDetail.setNonPriorityMsgId(nonPriorityMsgId);
							delayDetail.setPriorityMsgOffset(priorityMsgOffset);
							delayDetail.setNonPriorityMsgOffset(nonPriorityMsgOffset);
							delayDetail.setLastConsumedPriorityMsg(lastConsumedPriorityMsg == null ? null
									: JSON.toJSONString(new JsonPayloadCodec()
											.decode(lastConsumedPriorityMsg.getPayload(), Object.class)));
							delayDetail.setLastConsumedNonPriorityMsg(lastConsumedNonPriorityMsg == null ? null
									: JSON.toJSONString(new JsonPayloadCodec()
											.decode(lastConsumedNonPriorityMsg.getPayload(), Object.class)));
							topicDelayView.addDelay(delayDetail);

							topicDelayView.setTotalDelay(topicDelayView.getTotalDelay() + delay);
						}
					} catch (DalException e) {
						log.warn("Get delay of {}:{} failed.", t.getName(), p.getId(), e);
						continue;
					}
				}
			}
		}

		m_delays = delayMap;
		List<TopicDelayDetailView> list = new ArrayList<TopicDelayDetailView>(delayMap.values());
		Collections.sort(list, new Comparator<TopicDelayDetailView>() {
			@Override
			public int compare(TopicDelayDetailView o1, TopicDelayDetailView o2) {
				return o2.getTotalDelay() == o1.getTotalDelay() ? 0 : o2.getTotalDelay() > o1.getTotalDelay() ? 1 : -1;
			}
		});

		m_topDelays = list;
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
							// updateDelayDetails();
							updateTopDelays();
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
	public List<TopicDelayDetailView> getTopDelays(int top) {
		top = top > m_delays.size() ? m_delays.size() : top;
		return m_topDelays.subList(0, top > 0 ? top : 0);
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
