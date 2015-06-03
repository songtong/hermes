package com.ctrip.hermes.portal.service;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.service.storage.TopicStorageService;

@Named
public class ConsumerService {
	@Inject
	private MetaServiceWrapper m_metaService;

	@Inject
	private TopicStorageService m_storageService;

	public ConsumerGroup getConsumer(String topic, String consumer) {
		for (ConsumerGroup c : getConsumers(topic)) {
			if (c.getName().equals(consumer)) {
				return c;
			}
		}
		return null;
	}

	public List<ConsumerGroup> getConsumers(String topic) {
		return m_metaService.getMeta().getTopics().get(topic).getConsumerGroups();
	}

	public Map<String, List<ConsumerGroup>> getConsumers() {
		Map<String, List<ConsumerGroup>> map = new LinkedHashMap<String, List<ConsumerGroup>>();
		for (Entry<String, Topic> entry : m_metaService.getMeta().getTopics().entrySet()) {
			Collections.sort(entry.getValue().getConsumerGroups(), new Comparator<ConsumerGroup>() {
				@Override
				public int compare(ConsumerGroup cl, ConsumerGroup cr) {
					return cl.getName().compareTo(cr.getName());
				}
			});
			map.put(entry.getKey(), entry.getValue().getConsumerGroups());
		}
		return map;
	}

	public void deleteConsumerFromTopic(String topic, String consumer) throws Exception {
		Meta meta = m_metaService.getMeta();
		Topic t = meta.getTopics().get(topic);
		for (ConsumerGroup c : t.getConsumerGroups()) {
			if (c.getName().equals(consumer)) {
				t.getConsumerGroups().remove(c);
				m_storageService.delConsumerStorage(t, c);
				break;
			}
		}
		m_metaService.updateMeta(meta);
	}

	public ConsumerGroup addConsumerForTopic(String topic, ConsumerGroup consumer) throws Exception {
		Meta meta = m_metaService.getMeta();

		int maxConsumerId = 0;
		for (ConsumerGroup cg : meta.getTopics().get(topic).getConsumerGroups()) {
			if (cg.getId() != null && cg.getId() > maxConsumerId) {
				maxConsumerId = cg.getId();
			}
		}
		consumer.setId(maxConsumerId + 1);
		Topic t = meta.getTopics().get(topic);
		t.addConsumerGroup(consumer);

		m_storageService.addConsumerStorage(t, consumer);

		if (!m_metaService.updateMeta(meta)) {
			throw new RuntimeException("Update meta failed, please try later");
		}

		return consumer;
	}
}
