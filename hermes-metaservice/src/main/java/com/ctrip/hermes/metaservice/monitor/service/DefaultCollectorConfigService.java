package com.ctrip.hermes.metaservice.monitor.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.monitor.config.TopicCheckerConfig;
import com.ctrip.hermes.metaservice.monitor.config.TopicCheckerConfig.ConsumerCheckerConfig;
import com.ctrip.hermes.metaservice.service.KVService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.KVService.Tag;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Named(type = CollectorConfigService.class)
public class DefaultCollectorConfigService implements CollectorConfigService {
	private static final Logger log = LoggerFactory.getLogger(DefaultCollectorConfigService.class);

	private static final int DEFAULT_MAX_CHECKER_CACHE_SIZE = 5000;

	private static final long DEFAULT_CHECKER_LIST_INTERFAL_MILLI = TimeUnit.MINUTES.toMillis(10);

	@Inject
	private KVService m_kvService;

	@Inject
	private TopicService m_topicService;

	private long m_lastCheckerConfigListTimestamp = 0;

	private LoadingCache<String, TopicCheckerConfig> m_checkerConfigCache = CacheBuilder.newBuilder()
	      .maximumSize(DEFAULT_MAX_CHECKER_CACHE_SIZE) //
	      .build(new CacheLoader<String, TopicCheckerConfig>() {
		      @Override
		      public TopicCheckerConfig load(String topicName) throws Exception {
			      String value = m_kvService.getValue(topicName, Tag.CHECKER);
			      if (!StringUtils.isBlank(value)) {
				      return JSON.parseObject(value, TopicCheckerConfig.class);
			      } else {
				      return TopicCheckerConfig.fromTopic(m_topicService.findTopicEntityByName(topicName));
			      }
		      }
	      });

	private synchronized Map<String, TopicCheckerConfig> listTopicCheckerConfig() {
		if (System.currentTimeMillis() - m_lastCheckerConfigListTimestamp > DEFAULT_CHECKER_LIST_INTERFAL_MILLI) {
			Map<String, String> kvs = m_kvService.list(Tag.CHECKER);
			for (Entry<String, String> kv : kvs.entrySet()) {
				if (!StringUtils.isBlank(kv.getValue())) {
					m_checkerConfigCache.put(kv.getKey(), JSON.parseObject(kv.getValue(), TopicCheckerConfig.class));
				}
			}
			m_lastCheckerConfigListTimestamp = System.currentTimeMillis();
		}
		return m_checkerConfigCache.asMap();
	}

	private abstract class CheckerConfigTopicVisitor {
		public void visit() {
			for (Entry<String, TopicCheckerConfig> entry : listTopicCheckerConfig().entrySet()) {
				visitTopicConfig(entry.getValue());
			}
		}

		abstract public void visitTopicConfig(TopicCheckerConfig config);
	}

	private abstract class CheckerConfigConsumerVisitor {
		public void visit() {
			for (Entry<String, TopicCheckerConfig> entry : listTopicCheckerConfig().entrySet()) {
				for (Entry<String, ConsumerCheckerConfig> entry2 : entry.getValue().getConsumerCheckerConfigs().entrySet()) {
					visitConsumerConfig(entry.getKey(), entry2.getValue());
				}
			}
		}

		abstract public void visitConsumerConfig(String topic, ConsumerCheckerConfig config);
	}

	@Override
	public TopicCheckerConfig getTopicCheckerConfig(String topicName) {
		try {
			return m_checkerConfigCache.get(topicName);
		} catch (ExecutionException e) {
			log.error("Get topic checker config failed {}.", topicName, e);
		}
		return null;
	}

	@Override
	public void setTopicCheckerConfig(TopicCheckerConfig config) {
		if (config != null) {
			try {
				m_kvService.setKeyValue(config.getTopic(), JSON.toJSONString(config), Tag.CHECKER);
				m_checkerConfigCache.refresh(config.getTopic());
			} catch (Exception e) {
				log.error("Set topic monitor config failed: {}", config, e);
			}
		}
	}

	@Override
	public Map<String, Integer> listLargeDeadletterLimits() {
		final Map<String, Integer> map = new HashMap<>();
		new CheckerConfigTopicVisitor() {
			@Override
			public void visitTopicConfig(TopicCheckerConfig config) {
				if (config.isTopicLargeDeadLetterSwitch()) {
					map.put(config.getTopic(), config.getTopicLargeDeadLetterLimit());
				}
			}
		}.visit();
		return map;
	}

	@Override
	public Map<String, Integer> listLongTimeNoProduceLimits() {
		final Map<String, Integer> map = new HashMap<>();
		new CheckerConfigTopicVisitor() {
			@Override
			public void visitTopicConfig(TopicCheckerConfig config) {
				if (config.isLongTimeNoProduceSwitch()) {
					map.put(config.getTopic(), config.getLongTimeNoProduceLimit());
				}
			}
		}.visit();
		return map;
	}

	@Override
	public Map<String, Map<String, Integer>> listLongTimeNoConsumeLimits() {
		final Map<String, Map<String, Integer>> map = new HashMap<>();
		new CheckerConfigConsumerVisitor() {
			@Override
			public void visitConsumerConfig(String topic, ConsumerCheckerConfig config) {
				if (config.isLongTimeNoConsumeSwitch()) {
					Map<String, Integer> limits = map.get(topic);
					if (limits == null) {
						limits = new HashMap<>();
						map.put(topic, limits);
					}
					limits.put(config.getConsumer(), config.getLongTimeNoConsumeLimit());
				}
			}
		}.visit();
		return map;
	}

	@Override
	public Map<String, Map<String, Integer>> listConsumeLargeBacklogLimits() {
		final Map<String, Map<String, Integer>> map = new HashMap<>();
		new CheckerConfigConsumerVisitor() {
			@Override
			public void visitConsumerConfig(String topic, ConsumerCheckerConfig config) {
				if (config.isConsumeLargeBacklogSwitch()) {
					Map<String, Integer> limits = map.get(topic);
					if (limits == null) {
						limits = new HashMap<>();
						map.put(topic, limits);
					}
					limits.put(config.getConsumer(), config.getConsumeLargeBacklogLimit());
				}
			}
		}.visit();
		return map;
	}

	@Override
	public Map<String, Map<String, Integer>> listConsumeLargeDelayLimits() {
		final Map<String, Map<String, Integer>> map = new HashMap<>();
		new CheckerConfigConsumerVisitor() {
			@Override
			public void visitConsumerConfig(String topic, ConsumerCheckerConfig config) {
				if (config.isConsumeLargeDelaySwitch()) {
					Map<String, Integer> limits = map.get(topic);
					if (limits == null) {
						limits = new HashMap<>();
						map.put(topic, limits);
					}
					limits.put(config.getConsumer(), config.getConsumeLargeDelayLimit());
				}
			}
		}.visit();
		return map;
	}
}
