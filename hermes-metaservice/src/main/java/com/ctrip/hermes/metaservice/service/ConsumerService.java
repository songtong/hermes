package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;

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
	private CachedConsumerGroupDao m_consumerGroupDao;

	@Inject
	private CachedTopicDao m_topicDao;

	public synchronized ConsumerGroup addConsumerForTopics(Long topicId, ConsumerGroup consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = EntityToModelConverter.convert(consumer);
			consumerGroupModel.setTopicId(topicModel.getId());
			m_consumerGroupDao.insert(consumerGroupModel);
			consumer.setId(consumerGroupModel.getId());

			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				Topic topicEntity = ModelToEntityConverter.convert(topicModel);
				List<com.ctrip.hermes.meta.entity.ConsumerGroup> consumerGroups = findConsumerGroups(topicModel);
				for (com.ctrip.hermes.meta.entity.ConsumerGroup cg : consumerGroups) {
					topicEntity.addConsumerGroup(cg);
				}
				m_storageService.addConsumerStorage(topicEntity, consumer);
				m_zookeeperService.ensureConsumerLeaseZkPath(topicEntity);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
		return consumer;
	}

	public void deleteConsumerFromTopic(Long topicId, String consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);
			m_consumerGroupDao.deleteByPK(consumerGroup);
			com.ctrip.hermes.metaservice.model.Topic topic = m_topicDao.findByPK(topicId);
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				Topic topicEntity = ModelToEntityConverter.convert(topic);
				ConsumerGroup consumerGroupEntity = ModelToEntityConverter.convert(consumerGroup);
				m_storageService.delConsumerStorage(topicEntity, consumerGroupEntity);
				m_zookeeperService.deleteConsumerLeaseZkPath(topicEntity, consumerGroupEntity.getName());
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
	}

	public ConsumerGroup findConsumerGroup(Long topicId, String consumer) {
		try {
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroup = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer, ConsumerGroupEntity.READSET_FULL);
			return ModelToEntityConverter.convert(consumerGroup);
		} catch (Exception e) {
			logger.warn("findConsumerGroup failed",e);
		}
		return null;
	}

	public List<com.ctrip.hermes.meta.entity.ConsumerGroup> findConsumerGroups(
	      com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> models = m_consumerGroupDao
		      .findByTopic(topicModel.getId());
		List<com.ctrip.hermes.meta.entity.ConsumerGroup> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : models) {
			com.ctrip.hermes.meta.entity.ConsumerGroup entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	public Map<String, List<ConsumerGroup>> getConsumers() {
		Map<String, List<ConsumerGroup>> map = new LinkedHashMap<String, List<ConsumerGroup>>();
		try {
			Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> cgModels = m_consumerGroupDao.list();
			for (com.ctrip.hermes.metaservice.model.ConsumerGroup cgModel : cgModels) {
				com.ctrip.hermes.metaservice.model.Topic topic = m_topicDao.findByPK(cgModel.getTopicId());
				if (!map.containsKey(topic.getName())) {
					map.put(topic.getName(), new ArrayList<ConsumerGroup>());
				}
				if (map.containsKey(topic.getName())) {
					map.get(topic.getName()).add(ModelToEntityConverter.convert(cgModel));
				}
			}

		} catch (DalException e) {
			e.printStackTrace();
		}
		return map;
	}

	public List<ConsumerGroup> getConsumers(String topicName) {
		try {
			com.ctrip.hermes.metaservice.model.Topic topic = m_topicDao.findByName(topicName);
			return findConsumerGroups(topic);
		} catch (DalException e) {
			logger.warn("getConsumers failed", e);
		}
		return new ArrayList<ConsumerGroup>();
	}

	public synchronized ConsumerGroup updateGroupForTopic(Long topicId, ConsumerGroup consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.metaservice.model.ConsumerGroup originConsumer = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer.getName(), ConsumerGroupEntity.READSET_FULL);
			consumer.setId(originConsumer.getId());

			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = EntityToModelConverter.convert(consumer);
			m_consumerGroupDao.updateByPK(consumerGroupModel, ConsumerGroupEntity.UPDATESET_FULL);
			com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				Topic topicEntity = ModelToEntityConverter.convert(topicModel);
				List<com.ctrip.hermes.meta.entity.ConsumerGroup> consumerGroups = findConsumerGroups(topicModel);
				for (com.ctrip.hermes.meta.entity.ConsumerGroup cg : consumerGroups) {
					topicEntity.addConsumerGroup(cg);
				}
				m_zookeeperService.ensureConsumerLeaseZkPath(topicEntity);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
		return consumer;
	}
}
