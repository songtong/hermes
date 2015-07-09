package com.ctrip.hermes.portal.service.monitor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.dal.HermesPortalDao;
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

	// Map<Pair<Topic, Group-ID>, Map<Paritition-ID, Pair<Latest-prouced, Latest-consumed>>>
	private Map<Pair<String, Integer>, Map<Integer, Pair<Date, Date>>> m_delays = new HashMap<>();

	// key: topic, value: latest
	private Map<String, Date> m_latestProduced = new HashMap<>();

	// key: topic, value: ips
	private Map<String, List<String>> m_topic2producers = new HashMap<>();

	// key: topic, vlaue: <consumer, ips>
	private Map<String, Map<String, List<String>>> m_topic2consumers = new HashMap<>();

	// key:ip, value: topics
	private Map<String, List<String>> m_producer2topics = new HashMap<>();

	// key:ip, value:topics
	private Map<String, List<String>> m_consumer2topics = new HashMap<>();

	@Override
	public Date getLatestProduced(String topic) {
		Date date = m_latestProduced.get(topic);
		return date == null ? new Date(0) : date;
	}

	@Override
	public Map<String, List<String>> getTopic2ProducerIPs() {
		return m_topic2producers;
	}

	@Override
	public Map<String, Map<String, List<String>>> getTopic2ConsumerIPs() {
		return m_topic2consumers;
	}

	@Override
	public Map<String, List<String>> getConsumerIP2Topics() {
		return m_consumer2topics;
	}

	@Override
	public Map<String, List<String>> getProducerIP2Topics() {
		return m_producer2topics;
	}

	@Override
	public Pair<Date, Date> getDelay(String topic, int groupId) {
		Map<Integer, Pair<Date, Date>> delayDetail = getDelayDetails(topic, groupId);
		if (delayDetail != null) {
			Date latestProduced = new Date(0);
			Date latestConsumed = new Date(0);
			for (Entry<Integer, Pair<Date, Date>> entry : delayDetail.entrySet()) {
				if (entry.getValue().getKey().after(latestProduced)) {
					latestProduced = entry.getValue().getKey();
				}
				if (entry.getValue().getValue().after(latestConsumed)) {
					latestConsumed = entry.getValue().getValue();
				}
			}
			return new Pair<Date, Date>(latestProduced, latestConsumed);
		}
		log.warn("Delay information of {}:{} not found.", topic, groupId);
		return null;
	}

	// Map<Partition-ID, Pair<Latest-produced, Latest-consumed>>
	@Override
	public Map<Integer, Pair<Date, Date>> getDelayDetails(String topic, int groupId) {
		Map<Integer, Pair<Date, Date>> m = m_delays.get(new Pair<String, Integer>(topic, groupId));
		return m == null ? new HashMap<Integer, Pair<Date, Date>>() : m;
	}

	private void updateDelayDetails() {
		Map<Pair<String, Integer>, Map<Integer, Pair<Date, Date>>> m = new HashMap<>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			Topic t = entry.getValue();
			for (Partition p : t.getPartitions()) {
				for (ConsumerGroup c : t.getConsumerGroups()) {
					Pair<Date, Date> delay = null;
					try {
						delay = m_dao.getDelayTime(t.getName(), p.getId(), c.getId());
					} catch (DalException e) {
						log.warn("Get delay of {}:{}:{} failed.", t.getName(), p.getId(), PortalConstants.PRIORITY_TRUE, e);
						continue;
					}
					Pair<String, Integer> k = new Pair<String, Integer>(t.getName(), c.getId());
					if (!m.containsKey(k)) {
						m.put(k, new HashMap<Integer, Pair<Date, Date>>());
					}
					m.get(k).put(p.getId(), delay);
				}
			}
		}
		m_delays = m;
	}

	private void updateLatestProduced() {
		Map<String, Date> m = new HashMap<String, Date>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			String topic = entry.getValue().getName();
			Date current = m_latestProduced.get(topic) == null ? new Date(0) : m_latestProduced.get(topic);
			Date latest = new Date(current.getTime());
			for (Partition partition : m_metaService.findPartitionsByTopic(topic)) {
				try {
					latest = m_dao.getLatestProduced(topic, partition.getId());
				} catch (DalException e) {
					log.warn("Find latest produced failed. {}:{}", topic, partition.getId());
					continue;
				}
				current = latest.after(current) ? latest : current;
			}
			m.put(topic, current);
		}
		m_latestProduced = m;
	}

	private void updateProducerTopicRelationship() {
		Map<String, List<String>> topic2producers = new HashMap<String, List<String>>();
		for (String topic : m_metaService.getTopics().keySet()) {
			topic2producers.put(topic, m_elasticClient.getLastWeekProducers(topic));
		}
		m_topic2producers = topic2producers;

		Map<String, List<String>> producer2topics = new HashMap<String, List<String>>();
		for (Entry<String, List<String>> entry : topic2producers.entrySet()) {
			String topicName = entry.getKey();
			for (String ip : entry.getValue()) {
				List<String> topics = producer2topics.get(ip);
				if (topics == null) {
					producer2topics.put(ip, topics = new ArrayList<String>());
				}
				topics.add(topicName);
			}
		}
		m_producer2topics = producer2topics;
	}

	private void updateConsumerTopicRelationship() {
		Map<String, Map<String, List<String>>> topic2consumers = new HashMap<>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			String topic = entry.getKey();
			for (ConsumerGroup c : entry.getValue().getConsumerGroups()) {
				String consumer = c.getName();
				if (!topic2consumers.containsKey(topic)) {
					topic2consumers.put(topic, new HashMap<String, List<String>>());
				}
				topic2consumers.get(topic).put(consumer, m_elasticClient.getLastWeekConsumers(topic, consumer));
			}
		}
		m_topic2consumers = topic2consumers;

		Map<String, List<String>> consumer2topics = new HashMap<String, List<String>>();
		for (Entry<String, Map<String, List<String>>> entry : topic2consumers.entrySet()) {
			String topicName = entry.getKey();
			for (Entry<String, List<String>> ips : entry.getValue().entrySet()) {
				for (String ip : ips.getValue()) {
					List<String> topics = consumer2topics.get(ip);
					if (topics == null) {
						consumer2topics.put(ip, topics = new ArrayList<String>());
					}
					topics.add(topicName);
				}
			}
		}
		m_consumer2topics = consumer2topics;
	}

	@Override
	public void initialize() throws InitializationException {
		// updateDelayDetails();
		// updateLatestProduced();

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_MYSQL_UPDATE_TASK", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      updateDelayDetails();
				      updateLatestProduced();
			      }
		      }, 0, 1, TimeUnit.MINUTES);

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_ELASTIC_UPDATE_TASK", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      updateProducerTopicRelationship();
				      updateConsumerTopicRelationship();
			      }
		      }, 0, 30, TimeUnit.MINUTES);
	}
}
