package com.ctrip.hermes.metaservice.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;

@Named
public class ConsumerService {
	@Inject
	private PortalMetaService m_metaService;

	@Inject
	private TopicStorageService m_storageService;

	@Inject
	private ZookeeperService m_zookeeperService;

	@Inject
	private ConsumerGroupDao m_consumerGroupDao;

	public ConsumerGroup getConsumer(String topic, String consumer) {
		for (ConsumerGroup c : getConsumers(topic)) {
			if (c.getName().equals(consumer)) {
				return c;
			}
		}
		return null;
	}

	public List<ConsumerGroup> getConsumers(String topic) {
		return m_metaService.findConsumersByTopic(topic);
	}

	public Map<String, List<ConsumerGroup>> getConsumers() {
		Map<String, List<ConsumerGroup>> map = new LinkedHashMap<String, List<ConsumerGroup>>();
		try {
			List<Topic> topics = m_metaService.findTopics();
			for (Topic topic : topics) {
				map.put(topic.getName(), topic.getConsumerGroups());
			}
		} catch (DalException e) {
			e.printStackTrace();
		}
		return map;
	}

	public void deleteConsumerFromTopic(String topic, String consumer) throws Exception {
		Topic t = m_metaService.findTopicByName(topic);
		ConsumerGroup consumerGroupEntity = t.findConsumerGroup(consumer);
		com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroup = EntityToModelConverter.convert(consumerGroupEntity);
		if (consumerGroup != null) {
			boolean removed = m_consumerGroupDao.deleteByPK(consumerGroup) == 1 ? true : false;
			if (removed && Storage.MYSQL.equals(t.getStorageType())) {
				m_storageService.delConsumerStorage(t, consumerGroupEntity);
				m_zookeeperService.deleteConsumerLeaseZkPath(t, consumer);
			}
		}
	}

	public synchronized ConsumerGroup addConsumerForTopics(String topicName, ConsumerGroup consumer) throws Exception {
		Topic t = m_metaService.findTopicByName(topicName);
		com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = EntityToModelConverter.convert(consumer);
		consumerGroupModel.setTopicId(t.getId());
		m_consumerGroupDao.insert(consumerGroupModel);
		consumer.setId(consumerGroupModel.getId());
		
		if (Storage.MYSQL.equals(t.getStorageType())) {
			m_storageService.addConsumerStorage(t, consumer);
			m_zookeeperService.ensureConsumerLeaseZkPath(t);
		}

		return consumer;
	}

	public synchronized ConsumerGroup updateGroupForTopic(String topicName, ConsumerGroup c) throws Exception {
		Topic t = m_metaService.findTopicByName(topicName);
		ConsumerGroup originConsumer = t.findConsumerGroup(c.getName());
		c.setId(originConsumer.getId());

		com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = EntityToModelConverter.convert(c);
		m_consumerGroupDao.updateByPK(consumerGroupModel, ConsumerGroupEntity.UPDATESET_FULL);

		if (Storage.MYSQL.equals(t.getStorageType())) {
			m_zookeeperService.ensureConsumerLeaseZkPath(t);
		}
		return c;
	}
}
