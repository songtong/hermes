package com.ctrip.hermes.portal.service.dashboard;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.StreamingOutput;

import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
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
import com.ctrip.hermes.core.message.payload.PayloadCodecFactory;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.DeadLetter;
import com.ctrip.hermes.metaservice.queue.DeadLetterDao;
import com.ctrip.hermes.metaservice.queue.DeadLetterEntity;
import com.ctrip.hermes.metaservice.queue.ListUtils;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessageQueueDao;
import com.ctrip.hermes.metaservice.service.PartitionService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;
import com.ctrip.hermes.portal.service.meta.DefaultPortalMetaService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.netty.buffer.Unpooled;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

@Named(type = DashboardService.class)
public class DefaultDashboardService implements DashboardService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultDashboardService.class);

	private static final int CONSUMER_BACKLOG_CALCULATE_THREAD_COUNT = 10;

	private static final int CONSUMER_BACKLOG_CALCULATE_AWAITTIME_MINUTE = 1;

	@Inject
	private MessageQueueDao m_messageDao;

	@Inject
	private DeadLetterDao m_deadletterDao;

	@Inject
	private DefaultPortalMetaService m_metaService;

	@Inject
	private TopicService m_topicService;

	@Inject
	private PartitionService m_partitionService;

	@Inject
	private ClientEnvironment m_env;

	private Map<String, Date> m_latestProduced = new HashMap<>();

	private long m_timeStamp = -1;

	private Map<Tpg, String> m_consumerLeases = new HashMap<>();

	private Map<Pair<String, Integer>, String> m_brokerLeases = new HashMap<>();

	private Map<String, Map<String, Map<String, Long>>> m_topicOffsetLags = new HashMap<String, Map<String, Map<String, Long>>>();

	private ExecutorService m_es = Executors.newFixedThreadPool(CONSUMER_BACKLOG_CALCULATE_THREAD_COUNT);

	private Cache<Pair<String, String>, List<DelayDetail>> cachedConsumerBacklogs = CacheBuilder.newBuilder()
	      .maximumSize(20).expireAfterWrite(PortalConstants.CONSUMER_BACKLOG_EXPIRED_TIME_MIllIS, TimeUnit.MILLISECONDS)
	      .build();

	@Override
	public Date getLatestProduced(String topic) {
		Date date = m_latestProduced.get(topic);
		return date == null ? new Date(0) : date;
	}

	private String getMetaserverStatusString() {
		String metaServer = null;
		String url = String.format("http://%s:%s/%s", m_env.getMetaServerDomainName(), m_env.getGlobalConfig()
		      .getProperty("meta.port", "80").trim(), "metaserver/status");
		try {
			HttpResponse response = Request.Get(url).connectTimeout(2000).socketTimeout(5000).execute().returnResponse();
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
		if (metaServer == null) {
			return;
		}
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

	public void updateTopicLags() {
		List<String> brokers = Arrays.asList(m_env.getGlobalConfig().getProperty("kafka.bootstrap.servers").split(","));
		int port = Integer.parseInt(m_env.getGlobalConfig().getProperty("kafka.servers.port"));
		Map<String, Set<String>> topicConsumersIncluded = parseForTopicConsumerSettings(m_env.getGlobalConfig()
		      .getProperty("sitemon.kafka.topics"));

		if (topicConsumersIncluded != null) {
			Map<String, Map<String, Long>> offsetLags = getKafkaTopicsConsumersLags(brokers, port, topicConsumersIncluded);
			m_topicOffsetLags.put(Storage.KAFKA, offsetLags);
		}

		topicConsumersIncluded = parseForTopicConsumerSettings(m_env.getGlobalConfig()
		      .getProperty("sitemon.mysql.topics"));

		if (topicConsumersIncluded != null) {
			Map<String, Map<String, Long>> offsetLags = new HashMap<String, Map<String, Long>>();
			for (Map.Entry<String, Set<String>> topicConsumers : topicConsumersIncluded.entrySet()) {
				offsetLags.put(topicConsumers.getKey(),
				      getDelayForTopicConsumers(topicConsumers.getKey(), topicConsumers.getValue()));
			}
			m_topicOffsetLags.put(Storage.MYSQL, offsetLags);
		}
	}

	private Map<String, Set<String>> parseForTopicConsumerSettings(String settings) {
		Map<String, Set<String>> topicConsumers = new HashMap<String, Set<String>>();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonSettings;
		try {
			jsonSettings = mapper.readTree(settings);
		} catch (Exception e) {
			log.error("Failed to parse topic consumer settings in json format: {}", settings);
			return null;
		}

		Iterator<Map.Entry<String, JsonNode>> settingsIter = jsonSettings.getFields();
		while (settingsIter.hasNext()) {
			Map.Entry<String, JsonNode> setting = settingsIter.next();
			ArrayNode consumers = (ArrayNode) setting.getValue();
			Set<String> consumersSet = new HashSet<String>();
			for (JsonNode consumer : consumers) {
				consumersSet.add(consumer.asText());
			}
			topicConsumers.put(setting.getKey(), consumersSet);
		}
		return topicConsumers;
	}

	@Override
	public List<DelayDetail> getDelayDetailForConsumer(String topic, String consumer) {
		List<DelayDetail> consumerDelays = cachedConsumerBacklogs.getIfPresent(new Pair<String, String>(topic, consumer));
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

		Topic t = m_topicService.findTopicEntityByName(topic);
		CountDownLatch latch = new CountDownLatch(t.getPartitions().size());
		try {
			ConsumerGroup c = t.findConsumerGroup(consumer);
			if (Storage.MYSQL.equals(t.getStorageType())) {
				for (Partition p : t.getPartitions()) {
					m_es.execute(new ConsumerBacklogCalculateTask(topic, c, p.getId(), latch, m_messageDao, consumerDelays));
				}
				latch.await(CONSUMER_BACKLOG_CALCULATE_AWAITTIME_MINUTE, TimeUnit.MINUTES);
				for (DelayDetail delay : consumerDelays) {
					delay.setCurrentConsumerIp(m_consumerLeases.get(new Tpg(topic, delay.getPartitionId(), consumer)));
					delay.setCurrentBrokerIp(m_brokerLeases.get(new Pair<String, Integer>(topic, delay.getPartitionId())));
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

	public Map<String, Long> getDelayForTopicConsumers(String topic, Set<String> consumersIncluded) {
		Map<String, Long> consumerLags = new HashMap<String, Long>();
		Topic topicEntity = m_topicService.findTopicEntityByName(topic);
		for (ConsumerGroup consumerGroup : topicEntity.getConsumerGroups()) {
			if (!consumersIncluded.contains("*") && !consumersIncluded.contains(consumerGroup.getName())) {
				continue;
			}

			long sum = 0;
			List<DelayDetail> delays = getDelayDetailForConsumer(topic, consumerGroup.getName());
			for (DelayDetail delay : delays) {
				sum += delay.getNonPriorityDelay() + delay.getPriorityDelay() + delay.getResendDelay();
			}
			consumerLags.put(consumerGroup.getName(), sum);
		}
		return consumerLags;
	}

	private void updateLatestProduced() {
		Map<String, Date> m = new HashMap<String, Date>();
		for (Entry<String, Topic> entry : m_topicService.getTopicEntities().entrySet()) {
			Topic topic = entry.getValue();
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				String topicName = topic.getName();
				Date current = m_latestProduced.get(topicName) == null ? new Date(0) : m_latestProduced.get(topicName);
				Date latest = new Date(0);
				for (Partition partition : m_partitionService.findPartitionsByTopic(topic.getId())) {
					try {
						MessagePriority msgPriority = m_messageDao.getLatestProduced(topicName, partition.getId(),
						      PortalConstants.PRIORITY_TRUE);
						Date datePriority = msgPriority == null ? latest : msgPriority.getCreationDate();
						MessagePriority msgNonPriority = m_messageDao.getLatestProduced(topicName, partition.getId(),
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

	public Map<String, Map<String, Map<String, Long>>> getTopicOffsetLags() {
		return m_topicOffsetLags;
	}

	public Map<String, Map<String, Long>> getKafkaTopicsConsumersLags(List<String> brokers, int port,
	      Map<String, Set<String>> topicConsumersIncluded) {
		Map<String, Map<String, Long>> topicsOffsets = new HashMap<String, Map<String, Long>>();

		List<String> topics = new ArrayList<String>(topicConsumersIncluded.keySet());
		Map<String, Map<Integer, Long>> topicsPartitionsOffsets = getKafkaTopicsOffsets(brokers, port, topics);
		for (Map.Entry<String, Map<Integer, Long>> topicPartitionsOffsets : topicsPartitionsOffsets.entrySet()) {
			List<TopicAndPartition> tps = new ArrayList<TopicAndPartition>();
			for (Map.Entry<Integer, Long> offset : topicPartitionsOffsets.getValue().entrySet()) {
				tps.add(new TopicAndPartition(topicPartitionsOffsets.getKey(), offset.getKey()));
			}

			Topic topic = m_topicService.findTopicEntityByName(topicPartitionsOffsets.getKey());

			Map<String, Long> consumerGroupLags = new HashMap<String, Long>();
			for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
				Set<String> consumersIncluded = topicConsumersIncluded.get(topic.getName());
				if (!consumersIncluded.contains("*") && !consumersIncluded.contains(consumerGroup.getName())) {
					continue;
				}

				Map<String, Map<Integer, Long>> consumerOffsetsOnTopics = getConsumerOffsets(brokers, port, tps,
				      "consumerOffsetLookup", consumerGroup.getName());
				Map<Integer, Long> consumerOffsets = consumerOffsetsOnTopics.get(topicPartitionsOffsets.getKey());
				if (consumerOffsets == null) {
					continue;
				}
				long lag = 0;
				for (Map.Entry<Integer, Long> partitionOffset : topicPartitionsOffsets.getValue().entrySet()) {
					if (consumerOffsets.containsKey(partitionOffset.getKey())) {
						lag += partitionOffset.getValue() - consumerOffsets.get(partitionOffset.getKey());
					}
				}
				consumerGroupLags.put(consumerGroup.getName(), lag);
			}
			topicsOffsets.put(topicPartitionsOffsets.getKey(), consumerGroupLags);
		}
		return topicsOffsets;
	}

	private Map<String, Map<Integer, Long>> getKafkaTopicsOffsets(List<String> brokers, int port, List<String> topics) {
		Map<String, Map<Integer, Long>> topicsPartitionsOffsets = new HashMap<String, Map<Integer, Long>>();
		Map<String, TreeMap<Integer, PartitionMetadata>> metadatas = findLeader(brokers, port, topics);

		for (Entry<String, TreeMap<Integer, PartitionMetadata>> metadata : metadatas.entrySet()) {
			Map<Integer, Long> partition2Offsets = new HashMap<Integer, Long>();
			for (Entry<Integer, PartitionMetadata> entry : metadata.getValue().entrySet()) {
				int partition = entry.getKey();
				String leadBroker = entry.getValue().leader().host();
				String clientName = "Client_" + metadata.getKey();
				SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
				long readOffset = getLastOffset(consumer, metadata.getKey(), partition,
				      kafka.api.OffsetRequest.LatestTime(), clientName);

				partition2Offsets.put(partition, readOffset);
				if (consumer != null) {
					consumer.close();
				}
			}

			topicsPartitionsOffsets.put(metadata.getKey(), partition2Offsets);
		}

		return topicsPartitionsOffsets;
	}

	private Map<String, Map<Integer, Long>> getConsumerOffsets(List<String> brokers, int port,
	      List<TopicAndPartition> topicAndPartitions, String clientName, String groupId) {
		Map<String, Map<Integer, Long>> topicsPartitionsOffsets = new HashMap<String, Map<Integer, Long>>();
		for (String broker : brokers) {
			SimpleConsumer consumer = new SimpleConsumer(broker, port, 100000, 1024 * 1024, clientName);
			OffsetFetchRequest request = new OffsetFetchRequest(groupId, topicAndPartitions,
			      OffsetRequest.CurrentVersion(), 1, clientName);
			OffsetFetchResponse response = consumer.fetchOffsets(request);
			Map<TopicAndPartition, OffsetMetadataAndError> offsets = response.offsets();
			for (Map.Entry<TopicAndPartition, OffsetMetadataAndError> offset : offsets.entrySet()) {
				if (offset.getValue().error() == 0) {
					if (!topicsPartitionsOffsets.containsKey(offset.getKey().topic())) {
						topicsPartitionsOffsets.put(offset.getKey().topic(), new HashMap<Integer, Long>());
					}
					topicsPartitionsOffsets.get(offset.getKey().topic()).put(offset.getKey().partition(),
					      offset.getValue().offset());
				}
			}

			if (consumer != null) {
				consumer.close();
			}
		}
		return topicsPartitionsOffsets;
	}

	private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
		      kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			log.error("Error fetching data Offset Data the Broker. Reason: {}", response.errorCode(topic, partition));
			return 0;
		}

		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private Map<String, TreeMap<Integer, PartitionMetadata>> findLeader(List<String> seedBrokers, int port,
	      List<String> topics) {

		Map<String, TreeMap<Integer, PartitionMetadata>> map = new HashMap<String, TreeMap<Integer, PartitionMetadata>>();
		for (String seed : seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, port, 100000, 2 * 1024 * 1024, "leaderLookup");
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();

				for (TopicMetadata item : metaData) {
					TreeMap<Integer, PartitionMetadata> partitions = new TreeMap<Integer, PartitionMetadata>();
					for (PartitionMetadata part : item.partitionsMetadata()) {
						partitions.put(part.partitionId(), part);
					}
					map.put(item.topic(), partitions);
				}

				break;
			} catch (Exception e) {
				log.error("Error communicating with Broker {} to find Leader, Reason: ", seed, e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}

		return map;
	}

	@Override
	public void initialize() throws InitializationException {

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_MYSQL_UPDATE_TASK", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      try {
					      updateLatestProduced();
				      } catch (Throwable e) {
					      log.error("Update mysql monitor information failed.", e);
				      }
			      }
		      }, 0, 1, TimeUnit.MINUTES);

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_KAFKA_OFFSET_TASK", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      try {
					      updateTopicLags();
				      } catch (Throwable e) {
					      log.error("Update elastic monitor information failed.", e);
				      }
			      }
		      }, 0, 5, TimeUnit.MINUTES);

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

	@Override
	public List<DeadletterView> getLatestDeadLetter(String topic, String consumer, int count) throws DalException {
		Topic theTopic = m_topicService.findTopicEntityByName(topic);

		if (theTopic == null | theTopic.findConsumerGroup(consumer) == null || count <= 0) {
			return null;
		}

		ConsumerGroup theConsumer = theTopic.findConsumerGroup(consumer);

		@SuppressWarnings("unchecked")
		List<DeadLetter>[] lists = new List[theTopic.getPartitions().size()];
		for (int i = 0; i < theTopic.getPartitions().size(); i++) {
			Partition partition = theTopic.getPartitions().get(i);
			lists[i] = m_deadletterDao.findLatestByTpgAndCount(topic, partition.getId(), theConsumer.getId(), count,
			      DeadLetterEntity.READSET_FULL);
		}

		List<DeadLetter> topK = ListUtils.getTopK(count, new Comparator<DeadLetter>() {

			@Override
			public int compare(DeadLetter o1, DeadLetter o2) {
				return o1.getDeadDate().compareTo(o2.getDeadDate());
			}
		}, lists);

		@SuppressWarnings("unchecked")
		Collection<DeadletterView> collect = CollectionUtil.collect(topK, new Transformer() {

			@Override
			public Object transform(Object input) {
				// TODO Auto-generated method stub
				return new DeadletterView((DeadLetter) input);
			}
		});
		return new ArrayList<>(collect);
	}

	public class DeadletterView {

		private DeadLetter m_rawMessage;

		private String m_attributesString;

		private String m_payloadString;

		public DeadletterView(DeadLetter msg) {
			m_rawMessage = msg;
			HermesPrimitiveCodec codec = new HermesPrimitiveCodec(Unpooled.wrappedBuffer(msg.getAttributes()));
			m_attributesString = JSON.toJSONString(codec.readStringStringMap());
			if (Codec.JSON.equals(msg.getCodecType().split(",")[0])) {
				m_payloadString = JSON.toJSONString(PayloadCodecFactory.getCodecByType(msg.getCodecType()).decode(
				      msg.getPayload(), Object.class));
			} else if (Codec.AVRO.equals(msg.getCodecType().split(",")[0])) {
				m_payloadString = JSON.toJSONString(PayloadCodecFactory.getCodecByType(msg.getCodecType())
				      .decode(msg.getPayload(), GenericRecord.class).toString());
			}
		}

		public String getAttributesString() {
			return m_attributesString;
		}

		public String getPayloadString() {
			return m_payloadString;
		}

		public DeadLetter getRawMessage() {
			return m_rawMessage;
		}

	}

	private List<DeadLetter> getLatestDeadLettersByEndtimeAndTpg(String topic, int partition, int consumerId,
	      Date endTime, int count) throws DalException {
		return m_deadletterDao.findLatestByEndtimeAndTpg(topic, partition, consumerId, endTime, count,
		      DeadLetterEntity.READSET_FULL);
	}

	private List<DeadLetter> getDeadLettersByEndidAndStarttimeAndTpg(String topic, int partition, int consumerId,
	      long endId, Date startTime, int count) throws DalException {
		return m_deadletterDao.findLatestByEndidAndStarttimeAndTpg(topic, partition, consumerId, endId, startTime, count,
		      DeadLetterEntity.READSET_FULL);

	}

	@Override
	public StreamingOutput getDeadLetterStreamByTimespan(String topic, String consumer, final Date timeStart,
	      final Date timeEnd, final long sleepIntelval) {

		final Topic theTopic = m_topicService.findTopicEntityByName(topic);

		if (theTopic == null || theTopic.findConsumerGroup(consumer) == null || timeEnd.before(timeStart)
		      || timeStart.after(new Date(System.currentTimeMillis()))) {
			return null;
		}

		final ConsumerGroup theConsumer = theTopic.findConsumerGroup(consumer);

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream output) {
				OutputStreamWriter out = new OutputStreamWriter(output);

				Map<Integer, Long> latestIds = new HashMap<>();
				try {
					for (Partition partition : theTopic.getPartitions()) {
						List<DeadLetter> latestDeadLetters = getLatestDeadLettersByEndtimeAndTpg(theTopic.getName(),
						      partition.getId(), theConsumer.getId(), timeEnd, 1);
						if (latestDeadLetters == null || latestDeadLetters.isEmpty()) {
							latestIds.put(partition.getId(), null);
						} else {
							latestIds.put(partition.getId(), latestDeadLetters.get(0).getId());
						}
					}
				} catch (DalException e) {
					log.error(String.format("Failed to get deadletters from db for topic:{}, consumer:{}",
					      theTopic.getName(), theConsumer.getName()));
					throw new RuntimeException(String.format("Failed to get deadletters from db for topic:{}, consumer:{}",
					      theTopic.getName(), theConsumer.getName()));
				}

				try {
					out.write(String.format("%s,%s,%s,%s,%s,%s\n", "id", "ref_key", "payload", "partition_key",
					      "creation_date", "dead_date").toString());

					long counter = 0;
					int maxFlushSize = 1000;
					for (Partition partition : theTopic.getPartitions()) {
						while (latestIds.get(partition.getId()) != null) {
							List<DeadLetter> deadLetters = getDeadLettersByEndidAndStarttimeAndTpg(theTopic.getName(),
							      partition.getId(), theConsumer.getId(), latestIds.get(partition.getId()), timeStart,
							      maxFlushSize);
							if (deadLetters != null && !deadLetters.isEmpty()) {
								for (DeadLetter deadLetter : deadLetters) {
									String payload = null;
									if (Codec.JSON.equals(deadLetter.getCodecType().split(",")[0])) {
										payload = JSON.toJSONString(PayloadCodecFactory.getCodecByType(deadLetter.getCodecType())
										      .decode(deadLetter.getPayload(), Object.class));
									} else if (Codec.AVRO.equals(deadLetter.getCodecType().split(",")[0])) {
										payload = JSON.toJSONString(PayloadCodecFactory.getCodecByType(deadLetter.getCodecType())
										      .decode(deadLetter.getPayload(), GenericRecord.class).toString());
									}
									out.write(String.format("%s,%s,%s,%s,%s,%s\n", counter, deadLetter.getRefKey(), payload,
									      "partition_key", deadLetter.getCreationDate(), deadLetter.getDeadDate()));
									counter++;
								}

								out.flush();

								if (deadLetters.size() < maxFlushSize) {
									latestIds.put(partition.getId(), null);
								} else {
									latestIds.put(partition.getId(), deadLetters.get(deadLetters.size() - 1).getId() - 1);
								}
							} else {
								latestIds.put(partition.getId(), null);
							}
							
							try {
								Thread.sleep(sleepIntelval);
							} catch (InterruptedException e) {
								// do nothing
							}
						}

					}
					
					out.flush();
				} catch (IOException e) {
					throw new RuntimeException("Download failed: " + e.getMessage());
				} catch (DalException e) {
					log.error(String.format("Failed to get deadletters from db for topic:{}, consumer:{}",
					      theTopic.getName(), theConsumer.getName()));
					throw new RuntimeException(String.format("Failed to get deadletters from db for topic:{}, consumer:{}",
					      theTopic.getName(), theConsumer.getName()));
				}
			}
		};
		return stream;
	}
}
