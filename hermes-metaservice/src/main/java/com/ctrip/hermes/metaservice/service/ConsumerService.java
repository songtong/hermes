package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;

@Named
public class ConsumerService {

	@Inject
	private TopicStorageService m_storageService;

	@Inject
	private ZookeeperService m_zookeeperService;

	@Inject
	private CachedConsumerGroupDao m_consumerGroupDao;
	
	@Inject
	private TopicService m_topicService;

	public ConsumerGroup getConsumer(String topic, String consumer) {
		for (ConsumerGroup c : getConsumers(topic)) {
			if (c.getName().equals(consumer)) {
				return c;
			}
		}
		return null;
	}

	public List<ConsumerGroup> getConsumers(String topicName) {
		Topic topic = m_topicService.findTopicByName(topicName);
		return topic.getConsumerGroups();
	}

	public Map<String, List<ConsumerGroup>> getConsumers() {
		Map<String, List<ConsumerGroup>> map = new LinkedHashMap<String, List<ConsumerGroup>>();
		try {
			 Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> cgModels = m_consumerGroupDao.list();
			 for(com.ctrip.hermes.metaservice.model.ConsumerGroup cgModel:cgModels){
				 Topic topic = m_topicService.findTopicById(cgModel.getTopicId());
				 if(!map.containsKey(topic.getName())){
					 map.put(topic.getName(), new ArrayList<ConsumerGroup>());
				 }
				 if(map.containsKey(topic.getName())){
					 map.get(topic.getName()).add(ModelToEntityConverter.convert(cgModel));
				 }
			 }
			
		} catch (DalException e) {
			e.printStackTrace();
		}
		return map;
	}

	public void deleteConsumerFromTopic(String topic, String consumer) throws Exception {
		Topic t = m_topicService.findTopicByName(topic);
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
		Topic t = m_topicService.findTopicByName(topicName);
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
		Topic t = m_topicService.findTopicByName(topicName);
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
