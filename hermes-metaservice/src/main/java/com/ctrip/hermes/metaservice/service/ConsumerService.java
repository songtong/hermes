package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;

@Named
public class ConsumerService {
	@Inject
	private PortalMetaService m_metaService;

	@Inject
	private TopicStorageService m_storageService;

	@Inject
	private ZookeeperService m_zookeeperService;

	public ConsumerGroup getConsumer(String topic, String consumer) {
		for (ConsumerGroup c : getConsumers(topic)) {
			if (c.getName().equals(consumer)) {
				return c;
			}
		}
		return null;
	}

	public List<ConsumerGroup> getConsumers(String topic) {
		return new ArrayList<>(m_metaService.getMeta().getTopics().get(topic).getConsumerGroups());
	}

	public Map<String, List<ConsumerGroup>> getConsumers() {
		Map<String, List<ConsumerGroup>> map = new LinkedHashMap<String, List<ConsumerGroup>>();
		for (Entry<String, Topic> entry : m_metaService.getMeta().getTopics().entrySet()) {
			map.put(entry.getKey(), new ArrayList<>(entry.getValue().getConsumerGroups()));
		}
		return map;
	}

	public void deleteConsumerFromTopic(String topic, String consumer) throws Exception {
		Meta meta = m_metaService.getMeta();
		Topic t = meta.getTopics().get(topic);
		ConsumerGroup consumerGroup = t.findConsumerGroup(consumer);
		if (consumerGroup != null) {
			boolean removed = t.removeConsumerGroup(consumer);
			if (removed && Storage.MYSQL.equals(t.getStorageType())) {
				m_storageService.delConsumerStorage(t, consumerGroup);
				m_zookeeperService.deleteConsumerLeaseZkPath(t, consumer);
			}
		}
		m_metaService.updateMeta(meta);
	}

	public synchronized ConsumerGroup addConsumerForTopics(String topicName, ConsumerGroup consumer) throws Exception {
		Meta meta = m_metaService.getMeta();

		int maxConsumerId = 0;
		for (Entry<String, Topic> entry : meta.getTopics().entrySet()) {
			for (ConsumerGroup cg : entry.getValue().getConsumerGroups()) {
				if (cg.getId() != null && cg.getId() > maxConsumerId) {
					maxConsumerId = cg.getId();
				}
			}
		}

		consumer.setId(maxConsumerId + 1);
		Topic t = meta.getTopics().get(topicName);
		t.addConsumerGroup(consumer);
		if (Storage.MYSQL.equals(t.getStorageType())) {
			m_storageService.addConsumerStorage(t, consumer);
			m_zookeeperService.ensureConsumerLeaseZkPath(t);
		}

		if (!m_metaService.updateMeta(meta)) {
			throw new RuntimeException("Update meta failed, please try later");
		}

		return consumer;
	}

	public synchronized ConsumerGroup updateGroupForTopic(String topicName, ConsumerGroup c)
			throws Exception {
		Meta meta = m_metaService.getMeta();
		Topic t = meta.getTopics().get(topicName);
		ConsumerGroup originConsumer = t.findConsumerGroup(c.getName());
		c.setId(originConsumer.getId());
		t.removeConsumerGroup(c.getName());
		t.addConsumerGroup(c);
		if (Storage.MYSQL.equals(t.getStorageType())) {
			m_zookeeperService.ensureConsumerLeaseZkPath(t);
		}
		if (!m_metaService.updateMeta(meta)) {
			throw new RuntimeException("Update meta failed, please try later");
		}
		return c;
	}
}
