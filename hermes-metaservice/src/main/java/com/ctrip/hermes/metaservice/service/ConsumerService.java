package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.converter.ModelToViewConverter;
import com.ctrip.hermes.metaservice.converter.ViewToModelConverter;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
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
	private TopicService m_topicService;

	@Inject
	private ZookeeperService m_zookeeperService;

	@Inject
	private CachedConsumerGroupDao m_consumerGroupDao;

	@Inject
	private CachedTopicDao m_topicDao;

	public synchronized ConsumerGroupView addConsumerForTopics(Long topicId, ConsumerGroupView consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			Topic topic = m_topicService.findTopicEntityById(topicId);
			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = ViewToModelConverter.convert(consumer);
			consumerGroupModel.setTopicId(topic.getId());
			m_consumerGroupDao.insert(consumerGroupModel);
			consumer.setId(consumerGroupModel.getId());

			if (Storage.MYSQL.equals(topic.getStorageType())) {
				List<com.ctrip.hermes.meta.entity.ConsumerGroup> consumerGroups = findConsumerGroupEntities(topic.getId());
				for (com.ctrip.hermes.meta.entity.ConsumerGroup cg : consumerGroups) {
					topic.addConsumerGroup(cg);
				}
				ConsumerGroup consumerEntity = ModelToEntityConverter.convert(consumerGroupModel);
				m_storageService.addConsumerStorage(topic, consumerEntity);
				m_zookeeperService.ensureConsumerLeaseZkPath(topic);
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
			Topic topic = m_topicService.findTopicEntityById(topicId);
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				ConsumerGroup consumerGroupEntity = ModelToEntityConverter.convert(consumerGroup);
				m_storageService.delConsumerStorage(topic, consumerGroupEntity);
				m_zookeeperService.deleteConsumerLeaseZkPath(topic, consumerGroupEntity.getName());
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

	public List<ConsumerGroupView> findConsumerViews(Long topicId) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> models = m_consumerGroupDao.findByTopic(topicId,
		      false);
		List<ConsumerGroupView> views = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : models) {
			ConsumerGroupView view = ModelToViewConverter.convert(model);
			views.add(fillConsumerView(view, model.getTopicId()));
		}
		return views;
	}

	public List<ConsumerGroupView> getConsumerViews() throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> models = m_consumerGroupDao.list(false);
		List<ConsumerGroupView> views = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : models) {
			ConsumerGroupView view = ModelToViewConverter.convert(model);
			views.add(fillConsumerView(view, model.getTopicId()));
		}
		return views;
	}

	public synchronized ConsumerGroupView updateGroupForTopic(Long topicId, ConsumerGroupView consumer) throws Exception {
		try {
			tm.startTransaction("fxhermesmetadb");
			com.ctrip.hermes.metaservice.model.ConsumerGroup originConsumer = m_consumerGroupDao.findByTopicIdAndName(
			      topicId, consumer.getName(), ConsumerGroupEntity.READSET_FULL);
			consumer.setId(originConsumer.getId());

			com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = ViewToModelConverter.convert(consumer);
			m_consumerGroupDao.updateByPK(consumerGroupModel, ConsumerGroupEntity.UPDATESET_FULL);
			Topic topicModel = m_topicService.findTopicEntityById(topicId);
			if (Storage.MYSQL.equals(topicModel.getStorageType())) {
				List<com.ctrip.hermes.meta.entity.ConsumerGroup> consumerGroups = findConsumerGroupEntities(topicModel
				      .getId());
				for (com.ctrip.hermes.meta.entity.ConsumerGroup cg : consumerGroups) {
					topicModel.addConsumerGroup(cg);
				}
				m_zookeeperService.ensureConsumerLeaseZkPath(topicModel);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
		return consumer;
	}

	private ConsumerGroupView fillConsumerView(ConsumerGroupView view, Long topicId) throws DalException {
		com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao.findByPK(topicId);
		view.setTopicName(topicModel.getName());
		return view;
	}
}
