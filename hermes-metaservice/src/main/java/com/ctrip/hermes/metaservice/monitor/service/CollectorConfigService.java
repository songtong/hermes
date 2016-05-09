package com.ctrip.hermes.metaservice.monitor.service;

import java.util.Map;

import com.ctrip.hermes.metaservice.monitor.config.TopicCheckerConfig;

public interface CollectorConfigService {
	public TopicCheckerConfig getTopicCheckerConfig(String topicName);

	public void setTopicCheckerConfig(TopicCheckerConfig config);

	public Map<String, Integer> listLargeDeadletterLimits();

	public Map<String, Integer> listLongTimeNoProduceLimits();

	public Map<String, Map<String, Integer>> listLongTimeNoConsumeLimits();

	public Map<String, Map<String, Integer>> listConsumeLargeBacklogLimits();

	public Map<String, Map<String, Integer>> listConsumeLargeDelayLimits();
}
