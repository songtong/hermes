package com.ctrip.hermes.admin.core.service;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.hermes.admin.core.converter.ModelToEntityConverter;
import com.ctrip.hermes.admin.core.converter.ModelToViewConverter;
import com.ctrip.hermes.admin.core.converter.ViewToModelConverter;
import com.ctrip.hermes.admin.core.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.admin.core.dal.CachedPartitionDao;
import com.ctrip.hermes.admin.core.dal.CachedTopicDao;
import com.ctrip.hermes.admin.core.model.ConsumerGroupEntity;
import com.ctrip.hermes.admin.core.queue.MessagePriority;
import com.ctrip.hermes.admin.core.queue.MessagePriorityDao;
import com.ctrip.hermes.admin.core.queue.MessagePriorityEntity;
import com.ctrip.hermes.admin.core.queue.MessageQueueConstants;
import com.ctrip.hermes.admin.core.queue.OffsetMessage;
import com.ctrip.hermes.admin.core.queue.OffsetMessageDao;
import com.ctrip.hermes.admin.core.queue.OffsetMessageEntity;
import com.ctrip.hermes.admin.core.queue.OffsetResend;
import com.ctrip.hermes.admin.core.queue.OffsetResendDao;
import com.ctrip.hermes.admin.core.queue.OffsetResendEntity;
import com.ctrip.hermes.admin.core.queue.QueueType;
import com.ctrip.hermes.admin.core.queue.ResendGroupId;
import com.ctrip.hermes.admin.core.queue.ResendGroupIdDao;
import com.ctrip.hermes.admin.core.queue.ResendGroupIdEntity;
import com.ctrip.hermes.admin.core.service.storage.TopicStorageService;
import com.ctrip.hermes.admin.core.view.ConsumerGroupView;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.google.common.base.Charsets;

@Named
public class ConsumerService {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

	private static final int MetaServerConnectTimeout = 2000;

	private static final int MetaServerReadTimeout = 5000;

	@Inject
	private TransactionManager tm;

	@Inject
	private TopicStorageService m_storageService;

	@Inject
	private CachedPartitionDao m_partitionDao;

	@Inject
	private CachedConsumerGroupDao m_consumerGroupDao;

	@Inject
	private CachedTopicDao m_topicDao;

	@Inject
	private TopicService m_topicService;

	@Inject
	private ClientEnvironment m_env;

	@Inject
	private MessagePriorityDao m_messageDao;

	@Inject
	private OffsetMessageDao m_offsetDao;

	@Inject
	private ResendGroupIdDao m_resendDao;

	@Inject
	private OffsetResendDao m_offsetResendDao;

	private static final String ConsumerLeasesNodeName = "consumerLeases";

	public synchronized ConsumerGroupView addConsumerGroup(Long topicId, ConsumerGroupView consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.admin.core.model.Topic topicModel = m_topicDao.findByPK(topicId);
			com.ctrip.hermes.admin.core.model.ConsumerGroup consumerGroupModel = ViewToModelConverter.convert(consumer);
			consumerGroupModel.setTopicId(topicId);
			m_consumerGroupDao.insert(consumerGroupModel);
			consumer.setId(consumerGroupModel.getId());

			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				List<com.ctrip.hermes.admin.core.model.Partition> partitionModels = m_partitionDao.findByTopic(topicId,
				      false);
				m_storageService.addConsumerStorage(topicModel, partitionModels, consumerGroupModel);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
		return consumer;
	}

	public void deleteConsumerGroup(Long topicId, String consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.admin.core.model.Topic topicModel = m_topicDao.findByPK(topicId);
			List<com.ctrip.hermes.admin.core.model.ConsumerGroup> consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);
			if (!consumerGroup.isEmpty()) {
				m_consumerGroupDao.deleteByPK(consumerGroup.get(0));

				if (Storage.MYSQL.equals(topicModel.getStorageType())) {
					List<com.ctrip.hermes.admin.core.model.Partition> partitionModels = m_partitionDao.findByTopic(topicId,
					      false);
					m_storageService.delConsumerStorage(topicModel, partitionModels, consumerGroup.get(0));
				}
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
	}

	public ConsumerGroup findConsumerGroupEntity(Long topicId, String consumer) {
		try {
			List<com.ctrip.hermes.admin.core.model.ConsumerGroup> consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);

			if (consumerGroup.isEmpty()) {
				return null;
			}

			return ModelToEntityConverter.convert(consumerGroup.get(0));
		} catch (Exception e) {
			logger.warn("findConsumerGroup failed", e);
		}
		return null;
	}

	public List<ConsumerGroup> findConsumerGroupEntities(Long topicId) throws DalException {
		Collection<com.ctrip.hermes.admin.core.model.ConsumerGroup> models = m_consumerGroupDao.findByTopic(topicId,
		      false);
		List<com.ctrip.hermes.meta.entity.ConsumerGroup> entities = new ArrayList<>();
		for (com.ctrip.hermes.admin.core.model.ConsumerGroup model : models) {
			ConsumerGroup entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	public ConsumerGroupView findConsumerView(String topicName, String consumer) {
		com.ctrip.hermes.admin.core.model.Topic topicModel;
		try {
			topicModel = m_topicDao.findByName(topicName);
			if (topicModel != null) {
				return findConsumerView(topicModel.getId(), consumer);
			}
		} catch (DalException e) {
			logger.warn("findConsumerView by topicName & consumer failed.", e);
		}
		return null;
	}

	public ConsumerGroupView findConsumerView(Long topicId, String consumer) {
		try {
			List<com.ctrip.hermes.admin.core.model.ConsumerGroup> consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);

			if (consumerGroup.isEmpty()) {
				return null;
			}

			ConsumerGroupView view = ModelToViewConverter.convert(consumerGroup.get(0));
			fillConsumerView(consumerGroup.get(0).getTopicId(), view);
			return view;
		} catch (Exception e) {
			logger.warn("findConsumerGroupView failed", e);
		}
		return null;
	}

	public List<ConsumerGroupView> findConsumerViews(Long topicId) throws DalException {
		Collection<com.ctrip.hermes.admin.core.model.ConsumerGroup> models = m_consumerGroupDao.findByTopic(topicId,
		      false);
		List<ConsumerGroupView> views = new ArrayList<>();
		for (com.ctrip.hermes.admin.core.model.ConsumerGroup model : models) {
			ConsumerGroupView view = ModelToViewConverter.convert(model);
			views.add(fillConsumerView(model.getTopicId(), view));
		}
		return views;
	}

	public List<ConsumerGroupView> getConsumerViews() throws DalException {
		Collection<com.ctrip.hermes.admin.core.model.ConsumerGroup> models = m_consumerGroupDao.list(false);
		List<ConsumerGroupView> views = new ArrayList<>();
		for (com.ctrip.hermes.admin.core.model.ConsumerGroup model : models) {
			ConsumerGroupView view = ModelToViewConverter.convert(model);
			views.add(fillConsumerView(model.getTopicId(), view));
		}
		return views;
	}

	public synchronized ConsumerGroupView updateConsumerGroup(Long topicId, ConsumerGroupView consumer) throws Exception {
		try {
			List<com.ctrip.hermes.admin.core.model.ConsumerGroup> originConsumer = m_consumerGroupDao
			      .findByTopicIdAndName(topicId, consumer.getName(), ConsumerGroupEntity.READSET_FULL);

			if (originConsumer.isEmpty()) {
				return null;
			}

			consumer.setId(originConsumer.get(0).getId());
			com.ctrip.hermes.admin.core.model.ConsumerGroup consumerGroupModel = ViewToModelConverter.convert(consumer);

			m_consumerGroupDao.updateByPK(consumerGroupModel, ConsumerGroupEntity.UPDATESET_FULL);

		} catch (Exception e) {
			throw e;
		}
		return consumer;
	}

	public boolean isConsumerAlive(Topic topic, ConsumerGroup consumer) {
		Pair<Integer, String> responsePair = getRequestToMetaServer("metaserver/status", null);

		if (responsePair == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			logger.warn("Check consumer status failed! Load metaserver status from meta-servers faied.");
			throw new RuntimeException("Check consumer status failed! Load metaserver status from meta-servers faied.");
		}

		JSONObject metaServerJsonObject = JSON.parseObject(responsePair.getValue());
		try {
			JSONObject consumerLeases = metaServerJsonObject.getJSONObject(ConsumerLeasesNodeName);
			for (Entry<String, Object> entry : consumerLeases.entrySet()) {
				String[] splitKey = entry.getKey().split(",|=|]");
				if (splitKey.length != 6) {
					logger.warn("Parse comsumer lease for {} failed.", entry.getKey());
					continue;
				}
				String topicName = splitKey[1].trim();
				String consumerName = splitKey[5].trim();
				if (topicName.equals(topic.getName()) && consumerName.equals(consumer.getName())) {
					return true;
				}
			}
		} catch (Exception e) {
			logger.warn(
			      "Check consumer status failed! Parse consumer lease from metaserver status string failed Metaserver status string:{}!",
			      responsePair.getValue());
			throw new RuntimeException(
			      "Check consumer status failed! Parse consumer lease from metaserver status string failed!");
		}
		return false;
	}

	public void resetOffsetByTimestamp(String topicName, ConsumerGroup consumer, long timestamp) throws Exception {
		if (timestamp < 0) {
			throw new RuntimeException("Can not set offset timeStamp to: " + timestamp);
		}

		Map<Integer, Offset> messageOffsets = findMessageOffsetByTime(topicName, timestamp);

		for (Entry<Integer, Offset> messageOffset : messageOffsets.entrySet()) {
			doUpdateMessageOffset(topicName, messageOffset.getKey(), MessageQueueConstants.PRIORITY, consumer.getId(),
			      messageOffset.getValue().getPriorityOffset());
			doUpdateMessageOffset(topicName, messageOffset.getKey(), MessageQueueConstants.NON_PRIORITY, consumer.getId(),
			      messageOffset.getValue().getNonPriorityOffset());
			doUpdateResendOffsetToLatest(topicName, messageOffset.getKey(), consumer.getId());
		}
	}

	public void resetOffsetByShift(String topicName, ConsumerGroup consumerGroup, int partition, String queueType,
	      long shift) throws DalException {
		long latestMessageOffset;
		switch (QueueType.getQueueTypeByName(queueType)) {
		case PRIORITY:
			latestMessageOffset = doGetLatestMsgId(topicName, partition, MessageQueueConstants.PRIORITY);
			doUpdateMessageOffsetByShift(topicName, partition, MessageQueueConstants.PRIORITY, consumerGroup.getId(),
			      shift, latestMessageOffset);
			break;
		case NON_PRIORITY:
			latestMessageOffset = doGetLatestMsgId(topicName, partition, MessageQueueConstants.NON_PRIORITY);
			doUpdateMessageOffsetByShift(topicName, partition, MessageQueueConstants.NON_PRIORITY, consumerGroup.getId(),
			      shift, latestMessageOffset);
			break;
		case RESEND:
			latestMessageOffset = doGetLatestResendMsgId(topicName, partition, consumerGroup.getId());
			doUpdateResendOffsetByShift(topicName, partition, consumerGroup.getId(), shift, latestMessageOffset);
			break;
		default:
			logger.warn("Unknown queue type:{}, queue type should be one of:[{}, {}, {}]", queueType, QueueType.PRIORITY,
			      QueueType.NON_PRIORITY, QueueType.RESEND);
			throw new RuntimeException(String.format("Unknown queue type:%s, queue type should be one of:[%s, %s, %s]",
			      queueType, QueueType.PRIORITY, QueueType.NON_PRIORITY, QueueType.RESEND));
		}
	}

	public void updateOffsetToZero(String topicName, ConsumerGroup consumerGroup, int partition, String queueType)
	      throws DalException {
		switch (QueueType.getQueueTypeByName(queueType)) {
		case PRIORITY:
			doUpdateMessageOffset(topicName, partition, MessageQueueConstants.PRIORITY, consumerGroup.getId(), 0);
			break;
		case NON_PRIORITY:
			doUpdateMessageOffset(topicName, partition, MessageQueueConstants.NON_PRIORITY, consumerGroup.getId(), 0);
			break;
		case RESEND:
			doUpdateResendOffset(topicName, partition, consumerGroup.getId(), 0);
			break;
		default:
			logger.warn("Unknown queue type:{}, queue type should be one of:[{}, {}, {}]", queueType, QueueType.PRIORITY,
			      QueueType.NON_PRIORITY, QueueType.RESEND);
			throw new RuntimeException(String.format("Unknown queue type:%s, queue type should be one of:[%s, %s, %s]",
			      queueType, QueueType.PRIORITY, QueueType.NON_PRIORITY, QueueType.RESEND));
		}
	}

	@SuppressWarnings("unchecked")
	private Map<Integer, Offset> findMessageOffsetByTime(String topicName, long timestamp) {
		int retryTimes = 2;
		Exception exception = null;
		for (int i = 0; i < retryTimes; i++) {
			try {
				Map<String, String> params = new HashMap<String, String>();
				params.put("topic", topicName);
				params.put("partition", "-1");
				params.put("time", String.valueOf(timestamp));
				Pair<Integer, String> responsePair = getRequestToMetaServer("message/offset", params);
				if (responsePair == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
					logger.warn("Find message offset failed: [{}({}), {}], responsePair:{}.", topicName, -1, timestamp,
					      responsePair);
					throw new RuntimeException(String.format("Find message offset failed: [%s(%s), %s], responsePair:%s.",
					      topicName, -1, timestamp, responsePair));
				}
				String response = responsePair.getValue();
				try {
					Map<Integer, JSONObject> map = (Map<Integer, JSONObject>) JSON.parse(response);
					if (map != null) {
						return parseFromJsonObject(map);
					}
				} catch (Exception e) {
					logger.warn("Parse Offset object failed: [{}({}), {}], response:{}.", topicName, -1, timestamp,
					      response, e);
					throw new RuntimeException(String.format("Parse Offset object failed: [%s(%s), %s], response:%s.",
					      topicName, -1, timestamp, response), e);
				}
			} catch (Exception e) {
				exception = e;
			}
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// ignore it
			}
		}
		throw new RuntimeException("Find offset from metaserver failed.", exception);

	}

	public Pair<Integer, String> getRequestToMetaServer(final String path, final Map<String, String> requestParams) {

		String url = String.format("http://%s:%s/%s", m_env.getMetaServerDomainName(), m_env.getGlobalConfig()
		      .getProperty("meta.port", "80").trim(), path);
		InputStream is = null;
		try {
			if (requestParams != null) {
				String encodedRequestParamStr = encodePropertiesStr(requestParams);

				if (encodedRequestParamStr != null) {
					url = url + "?" + encodedRequestParamStr;
				}

			}

			HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

			conn.setConnectTimeout(MetaServerConnectTimeout);
			conn.setReadTimeout(MetaServerReadTimeout);
			conn.setRequestMethod("GET");
			conn.connect();

			int statusCode = conn.getResponseCode();

			if (statusCode == 200) {
				is = conn.getInputStream();
				return new Pair<Integer, String>(statusCode, IO.INSTANCE.readFrom(is, Charsets.UTF_8.name()));
			} else {
				return new Pair<Integer, String>(statusCode, null);
			}

		} catch (Exception e) {
			// ignore
			if (logger.isDebugEnabled()) {
				logger.debug("Request meta server error.", e);
			}
			return null;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (Exception e) {
					// ignore it
				}
			}
		}

	}

	private String encodePropertiesStr(Map<String, String> properties) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			sb.append(URLEncoder.encode(entry.getKey(), Charsets.UTF_8.name()))//
			      .append("=")//
			      .append(URLEncoder.encode(entry.getValue(), Charsets.UTF_8.name()))//
			      .append("&");
		}

		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return null;
		}
	}

	private Map<Integer, Offset> parseFromJsonObject(Map<Integer, JSONObject> map) {
		Map<Integer, Offset> result = new HashMap<Integer, Offset>();
		for (Entry<Integer, JSONObject> entry : map.entrySet()) {
			int partitionId = Integer.valueOf(String.valueOf(entry.getKey()));
			long pOffset = Long.valueOf(entry.getValue().get("priorityOffset").toString());
			long npOffset = Long.valueOf(entry.getValue().get("nonPriorityOffset").toString());
			result.put(partitionId, new Offset(pOffset, npOffset, null));
		}
		return result;
	}

	private long doGetLatestMsgId(String topicName, int partition, int priority) throws DalException {
		List<MessagePriority> latest = m_messageDao.latest(topicName, partition, priority,
		      MessagePriorityEntity.READSET_ID);
		long max = 0;
		if (latest != null && !latest.isEmpty()) {
			max = latest.get(0).getId();
		}
		return max;
	}

	private long doGetLatestResendMsgId(String topicName, int partition, int consumerId) throws DalException {
		List<ResendGroupId> latest = m_resendDao.latest(topicName, partition, consumerId,
		      ResendGroupIdEntity.READSET_FULL);
		long max = 0;
		if (latest != null && !latest.isEmpty()) {
			max = latest.get(0).getId();
		}
		return max;
	}

	private void doUpdateMessageOffsetByShift(String topicName, int partition, int priority, int consumerId, long shift,
	      long max) throws DalException {
		List<OffsetMessage> offsets = m_offsetDao.find(topicName, partition, priority, consumerId,
		      OffsetMessageEntity.READSET_FULL);
		OffsetMessage offset = null;
		if (CollectionUtil.isNullOrEmpty(offsets)) {
			offset = new OffsetMessage();
			offset.setGroupId(consumerId);
			offset.setOffset(correctOffset(max + shift, max, 0));
			offset.setPartition(partition);
			offset.setPriority(priority);
			offset.setTopic(topicName);
			m_offsetDao.insert(offset);
		} else {
			offset = offsets.get(0);
			offset.setOffset(correctOffset(offset.getOffset() + shift, max, 0));
			offset.setTopic(topicName);
			offset.setPartition(partition);
			m_offsetDao.updateByPK(offset, OffsetMessageEntity.UPDATESET_OFFSET);
		}
	}

	private void doUpdateMessageOffset(String topicName, int partition, int priority, int consumerId, long messageOffset)
	      throws DalException {
		List<OffsetMessage> offsets = m_offsetDao.find(topicName, partition, priority, consumerId,
		      OffsetMessageEntity.READSET_FULL);
		OffsetMessage offset = null;
		if (CollectionUtil.isNullOrEmpty(offsets)) {
			offset = new OffsetMessage();
			offset.setGroupId(consumerId);
			offset.setOffset(messageOffset);
			offset.setPartition(partition);
			offset.setPriority(priority);
			offset.setTopic(topicName);
			m_offsetDao.insert(offset);
		} else {
			offset = offsets.get(0);
			offset.setOffset(messageOffset);
			offset.setTopic(topicName);
			offset.setPartition(partition);
			m_offsetDao.updateByPK(offset, OffsetMessageEntity.UPDATESET_OFFSET);
		}
	}

	private void doUpdateResendOffsetToLatest(String topicName, int partition, int consumerId) throws DalException {
		List<ResendGroupId> latestResendOffset = m_resendDao.latest(topicName, partition, consumerId,
		      ResendGroupIdEntity.READSET_FULL);
		if (!CollectionUtil.isNullOrEmpty(latestResendOffset)) {
			ResendGroupId theLatesetResendOffset = latestResendOffset.get(0);
			List<OffsetResend> offsetResend = m_offsetResendDao.top(topicName, partition, consumerId,
			      OffsetResendEntity.READSET_FULL);
			if (!CollectionUtil.isNullOrEmpty(offsetResend)) {
				OffsetResend theOffsetResend = offsetResend.get(0);
				theOffsetResend.setTopic(topicName);
				theOffsetResend.setPartition(partition);
				theOffsetResend.setLastId(theLatesetResendOffset.getId());
				m_offsetResendDao.updateByPK(theOffsetResend, OffsetResendEntity.UPDATESET_OFFSET);
			} else {
				OffsetResend theOffsetResend = new OffsetResend();
				theOffsetResend.setTopic(topicName);
				theOffsetResend.setPartition(partition);
				theOffsetResend.setLastId(theLatesetResendOffset.getId());
				theOffsetResend.setGroupId(consumerId);
				theOffsetResend.setLastScheduleDate(new Date());
				m_offsetResendDao.insert(theOffsetResend);
			}
		}
	}

	private void doUpdateResendOffsetByShift(String topicName, int partition, int consumerId, long shift, long max)
	      throws DalException {
		if (max >= 0) {
			List<OffsetResend> offsetResends = m_offsetResendDao.top(topicName, partition, consumerId,
			      OffsetResendEntity.READSET_FULL);
			if (!CollectionUtil.isNullOrEmpty(offsetResends)) {
				OffsetResend offsetResend = offsetResends.get(0);
				offsetResend.setTopic(topicName);
				offsetResend.setPartition(partition);
				offsetResend.setLastId(correctOffset(offsetResend.getLastId() + shift, max, 0));
				m_offsetResendDao.updateByPK(offsetResend, OffsetResendEntity.UPDATESET_OFFSET);
			} else {
				OffsetResend theOffsetResend = new OffsetResend();
				theOffsetResend.setTopic(topicName);
				theOffsetResend.setPartition(partition);
				theOffsetResend.setLastId(correctOffset(max + shift, max, 0));
				theOffsetResend.setGroupId(consumerId);
				theOffsetResend.setLastScheduleDate(new Date());
				m_offsetResendDao.insert(theOffsetResend);
			}
		}
	}

	private void doUpdateResendOffset(String topicName, int partition, int consumerId, long offset) throws DalException {
		List<OffsetResend> offsetResends = m_offsetResendDao.top(topicName, partition, consumerId,
		      OffsetResendEntity.READSET_FULL);
		if (!CollectionUtil.isNullOrEmpty(offsetResends)) {
			OffsetResend offsetResend = offsetResends.get(0);
			offsetResend.setTopic(topicName);
			offsetResend.setPartition(partition);
			offsetResend.setLastId(offset);
			m_offsetResendDao.updateByPK(offsetResend, OffsetResendEntity.UPDATESET_OFFSET);
		} else {
			OffsetResend theOffsetResend = new OffsetResend();
			theOffsetResend.setTopic(topicName);
			theOffsetResend.setPartition(partition);
			theOffsetResend.setLastId(offset);
			theOffsetResend.setGroupId(consumerId);
			theOffsetResend.setLastScheduleDate(new Date());
			m_offsetResendDao.insert(theOffsetResend);
		}
	}

	private long correctOffset(long offset, long max, long min) {
		if (max < min || min < 0) {
			logger.warn("Invalid parameter: max={}, min={}.", max, min);
			throw new IllegalArgumentException(String.format("Invalid Max value %s or Min value %s.", max, min));
		}

		return offset > max ? max : offset < min ? min : offset;
	}

	private ConsumerGroupView fillConsumerView(Long topicId, ConsumerGroupView view) throws DalException {
		com.ctrip.hermes.admin.core.model.Topic topicModel = m_topicDao.findByPK(topicId);
		view.setTopicName(topicModel.getName());
		return view;
	}

}
