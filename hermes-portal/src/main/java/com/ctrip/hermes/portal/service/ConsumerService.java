package com.ctrip.hermes.portal.service;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;

@Named
public class ConsumerService {
	private static final Logger m_logger = LoggerFactory.getLogger(TopicService.class);

	@Inject
	private MetaServiceWrapper m_metaService;

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
}
