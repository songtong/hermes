package com.ctrip.hermes.metaservice.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.converter.ModelToViewConverter;
import com.ctrip.hermes.metaservice.converter.ViewToModelConverter;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.queue.MessageQueueConstants;
import com.ctrip.hermes.metaservice.queue.OffsetMessage;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessageEntity;
import com.ctrip.hermes.metaservice.queue.OffsetResend;
import com.ctrip.hermes.metaservice.queue.OffsetResendDao;
import com.ctrip.hermes.metaservice.queue.OffsetResendEntity;
import com.ctrip.hermes.metaservice.queue.ResendGroupId;
import com.ctrip.hermes.metaservice.queue.ResendGroupIdDao;
import com.ctrip.hermes.metaservice.queue.ResendGroupIdEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;

@Named
public class ConsumerService {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

	@Inject
	private TransactionManager tm;

	@Inject
	private TopicStorageService m_storageService;

	@Inject
	private ZookeeperService m_zookeeperService;

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
	private OffsetMessageDao m_offsetDao;

	@Inject
	private ResendGroupIdDao m_resendDao;

	@Inject
	private OffsetResendDao m_offsetResendDao;

	@Inject
	private MetaService m_metaService;

	private static final String ConsumerLeasesNodeName = "consumerLeases";

	public synchronized ConsumerGroupView addConsumerGroup(Long topicId, ConsumerGroupView consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = ViewToModelConverter.convert(consumer);
			consumerGroupModel.setTopicId(topicId);
			m_consumerGroupDao.insert(consumerGroupModel);
			consumer.setId(consumerGroupModel.getId());

			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				List<com.ctrip.hermes.metaservice.model.Partition> partitionModels = m_partitionDao.findByTopic(topicId,
				      false);
				m_storageService.addConsumerStorage(topicModel, partitionModels, consumerGroupModel);
				Topic topicEntity = m_topicService.findTopicEntityById(topicId);
				m_zookeeperService.ensureConsumerLeaseZkPath(topicEntity);
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
			com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);
			m_consumerGroupDao.deleteByPK(consumerGroup);

			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				List<com.ctrip.hermes.metaservice.model.Partition> partitionModels = m_partitionDao.findByTopic(topicId,
				      false);
				m_storageService.delConsumerStorage(topicModel, partitionModels, consumerGroup);
				Topic topicEntity = m_topicService.findTopicEntityById(topicId);
				m_zookeeperService.deleteConsumerLeaseZkPath(topicEntity, consumer);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
	}

	public ConsumerGroup findConsumerGroupEntity(Long topicId, String consumer) {
		try {
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);
			return ModelToEntityConverter.convert(consumerGroup);
		} catch (Exception e) {
			logger.warn("findConsumerGroup failed", e);
		}
		return null;
	}

	public List<ConsumerGroup> findConsumerGroupEntities(Long topicId) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> models = m_consumerGroupDao.findByTopic(topicId,
		      false);
		List<com.ctrip.hermes.meta.entity.ConsumerGroup> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : models) {
			ConsumerGroup entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	public ConsumerGroupView findConsumerView(Long topicId, String consumer) {
		try {
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);
			ConsumerGroupView view = ModelToViewConverter.convert(consumerGroup);
			fillConsumerView(consumerGroup.getTopicId(), view);
			return view;
		} catch (Exception e) {
			logger.warn("findConsumerGroupView failed", e);
		}
		return null;
	}

	public List<ConsumerGroupView> findConsumerViews(Long topicId) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> models = m_consumerGroupDao.findByTopic(topicId,
		      false);
		List<ConsumerGroupView> views = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : models) {
			ConsumerGroupView view = ModelToViewConverter.convert(model);
			views.add(fillConsumerView(model.getTopicId(), view));
		}
		return views;
	}

	public List<ConsumerGroupView> getConsumerViews() throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> models = m_consumerGroupDao.list(false);
		List<ConsumerGroupView> views = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : models) {
			ConsumerGroupView view = ModelToViewConverter.convert(model);
			views.add(fillConsumerView(model.getTopicId(), view));
		}
		return views;
	}

	public synchronized ConsumerGroupView updateConsumerGroup(Long topicId, ConsumerGroupView consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
			com.ctrip.hermes.metaservice.model.ConsumerGroup originConsumer = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer.getName(), ConsumerGroupEntity.READSET_FULL);
			consumer.setId(originConsumer.getId());
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = ViewToModelConverter.convert(consumer);
			m_consumerGroupDao.updateByPK(consumerGroupModel, ConsumerGroupEntity.UPDATESET_FULL);

			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				Topic topicEntity = m_topicService.findTopicEntityById(topicId);
				m_zookeeperService.ensureConsumerLeaseZkPath(topicEntity);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
		return consumer;
	}

	public boolean isConsumerAlive(Topic topic, ConsumerGroup consumer) {
		String metaServerString = null;
		String url = String.format("http://%s:%s/%s", m_env.getMetaServerDomainName(), m_env.getGlobalConfig()
		      .getProperty("meta.port", "80").trim(), "metaserver/status");
		try {
			HttpResponse response = Request.Get(url).execute().returnResponse();
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == HttpStatus.SC_OK) {
				metaServerString = EntityUtils.toString(response.getEntity());
			}
		} catch (IOException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Load metaserver status from meta-servers faied.", e);
			}
			throw new RuntimeException("Check consumer status failed! Load metaserver status from meta-servers faied.");
		}

		JSONObject metaServerJsonObject = JSON.parseObject(metaServerString);

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
		return false;
	}

	public void resetOffset(String topicName, ConsumerGroup consumer, long timestamp) throws Exception {
		if (timestamp < 0) {
			throw new RuntimeException("Can not set offset timeStamp to: " + timestamp);
		}

		Map<Integer, Offset> messageOffsets = findMessageOffsetByTime(topicName, timestamp);

		for (Entry<Integer, Offset> messageOffset : messageOffsets.entrySet()) {
			doUpdateMessageOffset(topicName, messageOffset.getKey(), MessageQueueConstants.PRIORITY_TRUE,
			      consumer.getId(), messageOffset.getValue().getPriorityOffset());
			doUpdateMessageOffset(topicName, messageOffset.getKey(), MessageQueueConstants.PRIORITY_FALSE,
			      consumer.getId(), messageOffset.getValue().getNonPriorityOffset());
			doUpdateResendOffset(topicName, messageOffset.getKey(), consumer.getId());
		}
	}

	public Map<Integer, Offset> findMessageOffsetByTime(String topicName, long timestamp) {
		int retryTimes = 2;
		Exception exception = null;
		for (int i = 0; i < retryTimes; i++) {
			try {
				return m_metaService.findMessageOffsetByTime(topicName, timestamp);
			} catch (Exception e) {
				exception = e;
			}
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// ignore it
			}
		}
		throw new RuntimeException("Find offset from metaserver failed: ", exception);

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

	private void doUpdateResendOffset(String topicName, int partition, int consumerId) throws DalException {
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

	private ConsumerGroupView fillConsumerView(Long topicId, ConsumerGroupView view) throws DalException {
		com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
		view.setTopicName(topicModel.getName());
		return view;
	}
}
