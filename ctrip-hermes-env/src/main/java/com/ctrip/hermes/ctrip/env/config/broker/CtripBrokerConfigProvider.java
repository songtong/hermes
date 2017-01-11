package com.ctrip.hermes.ctrip.env.config.broker;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.hermes.ctrip.env.StringUtils;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.env.config.broker.MySQLCacheConfigProvider;
import com.google.common.util.concurrent.RateLimiter;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerConfigProvider.class)
public class CtripBrokerConfigProvider implements BrokerConfigProvider, Initializable {

	private static final Logger log = LoggerFactory.getLogger(CtripBrokerConfigProvider.class);

	private static final String SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS = "sendMessageSelectorSafeTriggerIntervalMillis";

	private static final String SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS = "sendMessageSelectorSafeTriggerMinFireIntervalMillis";

	private static final String SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS = "sendMessageSelectorSafeTriggerTriggeringOffsetDeltas";

	private static final String SEND_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS = "sendMessageSelectorNormalTriggeringOffsetDeltas";

	private static final String PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS = "pullMessageSelectorSafeTriggerIntervalMillis";

	private static final String PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS = "pullMessageSelectorSafeTriggerMinFireIntervalMillis";

	private static final String PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS = "pullMessageSelectorSafeTriggerTriggeringOffsetDeltas";

	private static final String PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS = "pullMessageSelectorNormalTriggeringOffsetDeltas";

	private static final String RATE_LIMITS_QPS = "rateLimits.qps";

	private static final String RATE_LIMITS_BYTES = "rateLimits.bytes";

	private static final String FLUSH_LIMITS_COUNT = "flushLimits.count";

	private static final String FLUSH_LIMITS_AUTOMATION = "flushLimits.automatioin";

	private static final String DEFAULT_KEY = "_default_";

	@Inject
	private ClientEnvironment m_env;

	private String m_sessionId;

	private long m_leaseRenewTimeMillsBeforeExpire = 2 * 1000L;

	private int m_longPollingServiceThreadCount = 50;

	private static final int DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE = 5000;

	private int m_messageQueueFlushBatchSzie = DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE;

	private static final int DEFAULT_BROKER_PORT = 4376;

	private static final int DEFAULT_SHUTDOWN_PORT = 4888;

	private CtripMySQLCacheConfigProvider m_cacheConfig = new CtripMySQLCacheConfigProvider();

	private static final int DEFAULT_FILTER_PATTERN_CACHE_SIZE = 10000;

	private int m_filterPatternCacheSize = DEFAULT_FILTER_PATTERN_CACHE_SIZE;

	private static final int DEFAULT_FILTER_PER_TOPIC_CACHE_SIZE = 20;

	private int m_filterPerTopicCacheSize = DEFAULT_FILTER_PER_TOPIC_CACHE_SIZE;

	private static final int DEFAULT_FILTER_TOPIC_CACHE_SIZE = 5000;

	private static final int DEFAULT_SEND_MESSAGE_SELECTOR_NORMAL_TRIGGER_TRIGGERING_OFFSET_DELTA = 20;

	private static final int DEFAULT_SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA = 30;

	private static final int DEFAULT_PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTA = 1;

	private static final int DEFAULT_PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA = 1;

	private static final int DEFAULT_FLUSH_MESSAGE_LIMITS = 1000;

	private static final boolean DEFAULT_FLUSH_MESSAGE_LIMIT_AUTOMATION = false;

	private int m_filterTopicCacheSize = DEFAULT_FILTER_TOPIC_CACHE_SIZE;

	private AtomicReference<Map<String, Integer>> m_sendMessageSelectorNormalTriggeringOffsetDeltas = new AtomicReference<Map<String, Integer>>(
	      new HashMap<String, Integer>());

	private AtomicReference<Map<String, Integer>> m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = new AtomicReference<Map<String, Integer>>(
	      new HashMap<String, Integer>());

	private int m_sendMessageSelectorSafeTriggerMinFireIntervalMillis;

	private int m_pullMessageSelectorWriteOffsetTtlMillis = 8000;

	private int m_pullMessageSelectorSafeTriggerIntervalMillis;

	private int m_pullMessageSelectorOffsetLoaderThreadPoolSize = 50;

	private int m_pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds = 60;

	private int m_pullMessageSelectorSafeTriggerMinFireIntervalMillis;

	private int m_sendMessageSelectorSafeTriggerIntervalMillis;

	// Topic -> GroupId or default -> delta
	private AtomicReference<Map<String, Map<String, Integer>>> m_pullMessageSelectorNormalTriggeringOffsetDeltas = new AtomicReference<Map<String, Map<String, Integer>>>(
	      new HashMap<String, Map<String, Integer>>());

	private AtomicReference<Map<String, Map<String, Integer>>> m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = new AtomicReference<Map<String, Map<String, Integer>>>(
	      new HashMap<String, Map<String, Integer>>());

	private int m_messageQueueFlushThreadCount = 500;

	private long m_messageQueueFetchPriorityMessageBySafeTriggerMinInterval = 200;

	private long m_messageQueueFetchNonPriorityMessageBySafeTriggerMinInterval = 200;

	private long m_messageQueueFetchResendMessageBySafeTriggerMinInterval = 200;

	private AtomicReference<Map<String, Double>> m_topicQPSRateLimits = new AtomicReference<Map<String, Double>>(
	      new HashMap<String, Double>());

	private Map<Pair<String, Integer>, RateLimiter> m_topicPartitionQPSRateLimiters = new ConcurrentHashMap<>();

	private AtomicReference<Map<String, Double>> m_topicBytesRateLimits = new AtomicReference<Map<String, Double>>(
	      new HashMap<String, Double>());

	private Map<Pair<String, Integer>, RateLimiter> m_topicPartitionBytesRateLimiters = new ConcurrentHashMap<>();

	private AtomicReference<Map<String, Integer>> m_topicsFlushCountLimits = new AtomicReference<Map<String, Integer>>();

	private AtomicReference<Map<String, Boolean>> m_topicsFlushLimitAutomations = new AtomicReference<Map<String, Boolean>>();

	public CtripBrokerConfigProvider() {
		m_sessionId = System.getProperty("brokerId", UUID.randomUUID().toString());
	}

	@Override
	public void initialize() throws InitializationException {
		String flushBatchSizeStr = m_env.getGlobalConfig().getProperty("broker.flush.batch.size");
		if (StringUtils.isNumeric(flushBatchSizeStr)) {
			m_messageQueueFlushBatchSzie = Integer.valueOf(flushBatchSizeStr);
		}
		String flushThreadCountStr = m_env.getGlobalConfig().getProperty("broker.flush.thread.count");
		if (StringUtils.isNumeric(flushThreadCountStr)) {
			m_messageQueueFlushThreadCount = Integer.valueOf(flushThreadCountStr);
		}
		String messageQueueFetchPriorityMessageMinIntervalStr = m_env.getGlobalConfig().getProperty(
		      "broker.mq.priroty.msg.by.safe.trigger.min.fetch.interval");
		if (StringUtils.isNumeric(messageQueueFetchPriorityMessageMinIntervalStr)) {
			m_messageQueueFetchPriorityMessageBySafeTriggerMinInterval = Integer
			      .valueOf(messageQueueFetchPriorityMessageMinIntervalStr);
		}
		String messageQueueFetchNonPriorityMessageMinIntervalStr = m_env.getGlobalConfig().getProperty(
		      "broker.mq.nonpriroty.msg.by.safe.trigger.min.fetch.interval");
		if (StringUtils.isNumeric(messageQueueFetchNonPriorityMessageMinIntervalStr)) {
			m_messageQueueFetchNonPriorityMessageBySafeTriggerMinInterval = Integer
			      .valueOf(messageQueueFetchNonPriorityMessageMinIntervalStr);
		}
		String messageQueueFetchResendMessageMinIntervalStr = m_env.getGlobalConfig().getProperty(
		      "broker.mq.resend.msg.by.safe.trigger.min.fetch.interval");
		if (StringUtils.isNumeric(messageQueueFetchResendMessageMinIntervalStr)) {
			m_messageQueueFetchResendMessageBySafeTriggerMinInterval = Integer
			      .valueOf(messageQueueFetchResendMessageMinIntervalStr);
		}

		String longPollingServiceThreadCount = m_env.getGlobalConfig().getProperty(
		      "broker.long.polling.service.thread.count");
		if (StringUtils.isNumeric(longPollingServiceThreadCount)) {
			m_longPollingServiceThreadCount = Integer.valueOf(longPollingServiceThreadCount);
		}

		String pullMessageSelectorWriteOffsetTtlMillis = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.write.offset.ttl.millis");
		if (StringUtils.isNumeric(pullMessageSelectorWriteOffsetTtlMillis)) {
			m_pullMessageSelectorWriteOffsetTtlMillis = Integer.valueOf(pullMessageSelectorWriteOffsetTtlMillis);
		}

		String pullMessageSelectorOffsetLoaderThreadPoolSize = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.offset.loader.thread.pool.size");
		if (StringUtils.isNumeric(pullMessageSelectorOffsetLoaderThreadPoolSize)) {
			m_pullMessageSelectorOffsetLoaderThreadPoolSize = Integer
			      .valueOf(pullMessageSelectorOffsetLoaderThreadPoolSize);
		}

		String pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.offset.loader.thread.pool.keepalive.seconds");
		if (StringUtils.isNumeric(pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds)) {
			m_pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds = Integer
			      .valueOf(pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds);
		}

		String pullMessageSelectorSafeTriggerMinFireIntervalMillis = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.safe.trigger.min.fire.interval.millis");
		if (StringUtils.isNumeric(pullMessageSelectorSafeTriggerMinFireIntervalMillis)) {
			m_pullMessageSelectorSafeTriggerMinFireIntervalMillis = Integer
			      .valueOf(pullMessageSelectorSafeTriggerMinFireIntervalMillis);
		}

		m_cacheConfig.init(m_env.getGlobalConfig());

		initRateLimiters();

		initStorageFlushLimits();

		initSendMessageSelector();
		initPullMessageSelector();
	}

	private void initStorageFlushLimits() {

		Config config = ConfigService.getAppConfig();

		config.addChangeListener(new ConfigChangeListener() {

			@Override
			public void onChange(ConfigChangeEvent changeEvent) {
				if (changeEvent.changedKeys().contains(FLUSH_LIMITS_COUNT)) {
					Map<String, Integer> topicFlushCountLimits = parseTopicFlushLimits(changeEvent
					      .getChange(FLUSH_LIMITS_COUNT).getNewValue());
					if (topicFlushCountLimits != null) {
						m_topicsFlushCountLimits.set(topicFlushCountLimits);
						log.info("{} changed({}).", FLUSH_LIMITS_COUNT, JSON.toJSONString(topicFlushCountLimits));
					}
				}
				if (changeEvent.changedKeys().contains(FLUSH_LIMITS_AUTOMATION)) {
					Map<String, Boolean> topicFlushLimitAutomations = parseTopicAutomations(changeEvent.getChange(
					      FLUSH_LIMITS_AUTOMATION).getNewValue());
					if (topicFlushLimitAutomations != null) {
						m_topicsFlushLimitAutomations.set(topicFlushLimitAutomations);
						log.info("{} changed({}).", FLUSH_LIMITS_AUTOMATION, topicFlushLimitAutomations);
					}
				}
			}
		});

		Map<String, Integer> topicFlushCountLimits = parseTopicFlushLimits(config.getProperty(FLUSH_LIMITS_COUNT, "{}"));
		if (topicFlushCountLimits != null) {
			m_topicsFlushCountLimits.set(topicFlushCountLimits);
			log.info("{} is {}.", FLUSH_LIMITS_COUNT, JSON.toJSONString(topicFlushCountLimits));
		}
		Map<String, Boolean> topicFlushLimitAutomations = parseTopicAutomations(config.getProperty(
		      FLUSH_LIMITS_AUTOMATION, "{}"));
		if (topicFlushLimitAutomations != null) {
			m_topicsFlushLimitAutomations.set(topicFlushLimitAutomations);
			log.info("{} is {}.", FLUSH_LIMITS_AUTOMATION, JSON.toJSONString(topicFlushLimitAutomations));
		}

	}

	private void initPullMessageSelector() {
		Config config = ConfigService.getAppConfig();

		config.addChangeListener(new ConfigChangeListener() {

			@Override
			public void onChange(ConfigChangeEvent changeEvent) {
				if (changeEvent.changedKeys().contains(PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS)) {
					try {
						m_pullMessageSelectorSafeTriggerMinFireIntervalMillis = Integer.parseInt(changeEvent.getChange(
						      PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS).getNewValue());
						log.info("PullMessageSelector safeTrigger minFireIntervalMillis changed ({})",
						      m_pullMessageSelectorSafeTriggerMinFireIntervalMillis);
					} catch (Exception e) {
						log.error("Parse pullMessageSelector safeTrigger minFireIntervalMillis failed.", e);
					}
				}

				if (changeEvent.changedKeys().contains(PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS)) {
					try {
						m_pullMessageSelectorSafeTriggerIntervalMillis = Integer.parseInt(changeEvent.getChange(
						      PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS).getNewValue());
						log.info("PullMessageSelector safeTrigger intervalMillis changed ({})",
						      m_pullMessageSelectorSafeTriggerIntervalMillis);
					} catch (Exception e) {
						log.error("Parse pullMessageSelector safeTrigger intervalMillis failed.", e);
					}
				}

				if (changeEvent.changedKeys().contains(PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS)) {
					Map<String, Map<String, Integer>> pullMessageSelectorNormalTriggeringOffsetDeltas = parsePullMessageSelectorDeltas(changeEvent
					      .getChange(PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS).getNewValue());
					if (pullMessageSelectorNormalTriggeringOffsetDeltas != null) {
						m_pullMessageSelectorNormalTriggeringOffsetDeltas
						      .set(pullMessageSelectorNormalTriggeringOffsetDeltas);
						log.info("PullMessageSelector normalTrigger offset deltas changed({})",
						      JSON.toJSON(m_pullMessageSelectorNormalTriggeringOffsetDeltas.get()));
					}
				}

				if (changeEvent.changedKeys().contains(PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS)) {
					Map<String, Map<String, Integer>> pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = parsePullMessageSelectorDeltas(changeEvent
					      .getChange(PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS).getNewValue());
					if (pullMessageSelectorSafeTriggerTriggeringOffsetDeltas != null) {
						m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas
						      .set(pullMessageSelectorSafeTriggerTriggeringOffsetDeltas);
						log.info("PullMessageSelector safteTrigger offset deltas changed({})",
						      JSON.toJSON(m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas.get()));
					}
				}
			}
		});

		m_pullMessageSelectorSafeTriggerMinFireIntervalMillis = config.getIntProperty(
		      PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS, 1000);
		log.info("PullMessageSelector safeTrigger minFireIntervalMillis is {}",
		      m_pullMessageSelectorSafeTriggerMinFireIntervalMillis);

		m_pullMessageSelectorSafeTriggerIntervalMillis = config.getIntProperty(
		      PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS, 1000);
		log.info("PullMessageSelector safeTrigger intervalMillis is {}", m_pullMessageSelectorSafeTriggerIntervalMillis);

		Map<String, Map<String, Integer>> pullMessageSelectorNormalTriggeringOffsetDeltas = parsePullMessageSelectorDeltas(config
		      .getProperty(PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS, "{}"));
		if (pullMessageSelectorNormalTriggeringOffsetDeltas != null) {
			m_pullMessageSelectorNormalTriggeringOffsetDeltas.set(pullMessageSelectorNormalTriggeringOffsetDeltas);
			log.info("PullMessageSelector normalTrigger offset deltas is {}",
			      JSON.toJSON(m_pullMessageSelectorNormalTriggeringOffsetDeltas.get()));
		}

		Map<String, Map<String, Integer>> pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = parsePullMessageSelectorDeltas(config
		      .getProperty(PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS, "{}"));
		if (pullMessageSelectorSafeTriggerTriggeringOffsetDeltas != null) {
			m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas
			      .set(pullMessageSelectorSafeTriggerTriggeringOffsetDeltas);
			log.info("PullMessageSelector safteTrigger offset deltas is {}",
			      JSON.toJSON(m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas.get()));
		}
	}

	private Map<String, Map<String, Integer>> parsePullMessageSelectorDeltas(String deltasStr) {
		try {
			return JSON.parseObject(deltasStr, new TypeReference<Map<String, Map<String, Integer>>>() {
			});
		} catch (Exception e) {
			log.error("Parse pull selector's delta failed.", e);
		}
		return null;
	}

	private void initSendMessageSelector() {
		Config config = ConfigService.getAppConfig();

		config.addChangeListener(new ConfigChangeListener() {

			@Override
			public void onChange(ConfigChangeEvent changeEvent) {
				if (changeEvent.changedKeys().contains(SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS)) {
					try {
						m_sendMessageSelectorSafeTriggerMinFireIntervalMillis = Integer.parseInt(changeEvent.getChange(
						      SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS).getNewValue());
						log.info("SendMessageSelector safeTrigger minFireIntervalMillis changed ({})",
						      m_sendMessageSelectorSafeTriggerMinFireIntervalMillis);
					} catch (Exception e) {
						log.error("Parse sendMessageSelector safeTrigger minFireIntervalMillis failed.", e);
					}
				}

				if (changeEvent.changedKeys().contains(SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS)) {
					try {
						m_sendMessageSelectorSafeTriggerIntervalMillis = Integer.parseInt(changeEvent.getChange(
						      SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS).getNewValue());
						log.info("SendMessageSelector safeTrigger intervalMillis is {}",
						      m_sendMessageSelectorSafeTriggerIntervalMillis);
					} catch (Exception e) {
						log.error("Parse sendMessageSelector safeTrigger intervalMillis failed.", e);
					}
				}

				if (changeEvent.changedKeys().contains(SEND_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS)) {
					Map<String, Integer> sendMessageSelectorNormalTriggeringOffsetDeltas = parseSendMessageSelectorDeltas(changeEvent
					      .getChange(SEND_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS).getNewValue());
					if (sendMessageSelectorNormalTriggeringOffsetDeltas != null) {
						m_sendMessageSelectorNormalTriggeringOffsetDeltas
						      .set(sendMessageSelectorNormalTriggeringOffsetDeltas);
						log.info("SendMessageSelector normalTrigger offset deltas changed({})",
						      JSON.toJSON(m_sendMessageSelectorNormalTriggeringOffsetDeltas.get()));
					}
				}

				if (changeEvent.changedKeys().contains(SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS)) {
					Map<String, Integer> sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = parseSendMessageSelectorDeltas(changeEvent
					      .getChange(SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS).getNewValue());
					if (sendMessageSelectorSafeTriggerTriggeringOffsetDeltas != null) {
						m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas
						      .set(sendMessageSelectorSafeTriggerTriggeringOffsetDeltas);
						log.info("SendMessageSelector safteTrigger offset deltas changed({})",
						      JSON.toJSON(m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas.get()));
					}
				}
			}
		});

		m_sendMessageSelectorSafeTriggerMinFireIntervalMillis = config.getIntProperty(
		      SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_MIN_FIRE_INTERVAL_MILLIS, 10);
		log.info("SendMessageSelector safeTrigger minFireIntervalMillis is {}",
		      m_sendMessageSelectorSafeTriggerMinFireIntervalMillis);

		m_sendMessageSelectorSafeTriggerIntervalMillis = config.getIntProperty(
		      SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_INTERVAL_MILLIS, 10);
		log.info("SendMessageSelector safeTrigger intervalMillis is {}", m_sendMessageSelectorSafeTriggerIntervalMillis);

		Map<String, Integer> sendMessageSelectorNormalTriggeringOffsetDeltas = parseSendMessageSelectorDeltas(config
		      .getProperty(SEND_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTAS, "{}"));
		if (sendMessageSelectorNormalTriggeringOffsetDeltas != null) {
			m_sendMessageSelectorNormalTriggeringOffsetDeltas.set(sendMessageSelectorNormalTriggeringOffsetDeltas);
			log.info("SendMessageSelector normalTrigger offset deltas is {}",
			      JSON.toJSON(m_sendMessageSelectorNormalTriggeringOffsetDeltas.get()));
		}

		Map<String, Integer> sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = parseSendMessageSelectorDeltas(config
		      .getProperty(SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTAS, "{}"));
		if (sendMessageSelectorSafeTriggerTriggeringOffsetDeltas != null) {
			m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas
			      .set(sendMessageSelectorSafeTriggerTriggeringOffsetDeltas);
			log.info("SendMessageSelector safteTrigger offset deltas is {}",
			      JSON.toJSON(m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas.get()));
		}
	}

	private Map<String, Integer> parseSendMessageSelectorDeltas(String deltasStr) {
		try {
			return JSON.parseObject(deltasStr, new TypeReference<Map<String, Integer>>() {
			});
		} catch (Exception e) {
			log.error("Parse send selector's delta failed.", e);
		}
		return null;
	}

	private void initRateLimiters() {
		Config config = ConfigService.getAppConfig();

		config.addChangeListener(new ConfigChangeListener() {

			@Override
			public void onChange(ConfigChangeEvent changeEvent) {
				if (changeEvent.changedKeys().contains(RATE_LIMITS_QPS)) {
					Map<String, Double> topicQPSRateLimits = parseTopicLimits(changeEvent.getChange(RATE_LIMITS_QPS)
					      .getNewValue());
					if (topicQPSRateLimits != null) {
						m_topicQPSRateLimits.set(topicQPSRateLimits);
						log.info("rateLimits.qps changed({}).", topicQPSRateLimits);
					}
				}
				if (changeEvent.changedKeys().contains(RATE_LIMITS_BYTES)) {
					Map<String, Double> topicBytesRateLimits = parseTopicLimits(changeEvent.getChange(RATE_LIMITS_BYTES)
					      .getNewValue());
					if (topicBytesRateLimits != null) {
						m_topicBytesRateLimits.set(topicBytesRateLimits);
						log.info("rateLimits.bytes changed({}).", topicBytesRateLimits);
					}
				}
			}
		});

		Map<String, Double> topicQPSRateLimits = parseTopicLimits(config.getProperty(RATE_LIMITS_QPS, "{}"));
		if (topicQPSRateLimits != null) {
			m_topicQPSRateLimits.set(topicQPSRateLimits);
			log.info("rateLimits.qps is {}.", JSON.toJSONString(topicQPSRateLimits));
		}
		Map<String, Double> topicBytesRateLimits = parseTopicLimits(config.getProperty(RATE_LIMITS_BYTES, "{}"));
		if (topicBytesRateLimits != null) {
			m_topicBytesRateLimits.set(topicBytesRateLimits);
			log.info("rateLimits.bytes is {}.", JSON.toJSONString(topicBytesRateLimits));
		}
	}

	private Map<String, Double> parseTopicLimits(String limitsStr) {
		try {
			Map<String, Double> topicLimits = JSON.parseObject(limitsStr, new TypeReference<Map<String, Double>>() {
			});
			return topicLimits;
		} catch (Exception e) {
			log.error("Parse limits failed.", e);
		}
		return null;
	}

	private Map<String, Integer> parseTopicFlushLimits(String limitsStr) {
		try {
			Map<String, Integer> topicLimits = JSON.parseObject(limitsStr, new TypeReference<Map<String, Integer>>() {
			});
			return topicLimits;
		} catch (Exception e) {
			log.error("Parse limits failed.", e);
		}
		return null;
	}

	private Map<String, Boolean> parseTopicAutomations(String automationsStr) {
		try {
			Map<String, Boolean> topicAutomations = JSON.parseObject(automationsStr,
			      new TypeReference<Map<String, Boolean>>() {
			      });
			return topicAutomations;
		} catch (Exception e) {
			log.error("Parse automations failed.", e);
		}
		return null;
	}

	public String getSessionId() {
		return m_sessionId;
	}

	public String getRegistryName(String name) {
		return "default";
	}

	public String getRegistryBasePath() {
		return "brokers";
	}

	public long getLeaseRenewTimeMillsBeforeExpire() {
		return m_leaseRenewTimeMillsBeforeExpire;
	}

	public int getLongPollingServiceThreadCount() {
		return m_longPollingServiceThreadCount;
	}

	public int getMessageQueueFlushBatchSize() {
		return m_messageQueueFlushBatchSzie;
	}

	public int getMessageQueueFlushCountLimit(String topic) {
		Map<String, Integer> topicFlushCountLimits = m_topicsFlushCountLimits.get();
		if (topicFlushCountLimits.containsKey(topic)) {
			return topicFlushCountLimits.get(topic);
		} else if (topicFlushCountLimits.containsKey(DEFAULT_KEY)) {
			return topicFlushCountLimits.get(DEFAULT_KEY);
		} else {
			return DEFAULT_FLUSH_MESSAGE_LIMITS;
		}

	}

	public boolean isMessageQueueFlushLimitAutomated(String topic) {
		Map<String, Boolean> topicFlushCountLimitAutomations = m_topicsFlushLimitAutomations.get();
		if (topicFlushCountLimitAutomations.containsKey(topic)) {
			return topicFlushCountLimitAutomations.get(topic);
		} else if (topicFlushCountLimitAutomations.containsKey(DEFAULT_KEY)) {
			return topicFlushCountLimitAutomations.get(DEFAULT_KEY);
		} else {
			return DEFAULT_FLUSH_MESSAGE_LIMIT_AUTOMATION;
		}
	}

	public long getAckOpCheckIntervalMillis() {
		return 200;
	}

	public int getAckOpHandlingBatchSize() {
		return 5000;
	}

	public int getAckOpExecutorThreadCount() {
		return 10;
	}

	public int getAckOpQueueSize() {
		return 500000;
	}

	public int getLeaseContainerThreadCount() {
		return 10;
	}

	public long getDefaultLeaseRenewDelayMillis() {
		return 500L;
	}

	public long getDefaultLeaseAcquireDelayMillis() {
		return 100L;
	}

	public int getListeningPort() {
		String port = System.getProperty("brokerPort");
		if (!StringUtils.isNumeric(port)) {
			return DEFAULT_BROKER_PORT;
		} else {
			return Integer.valueOf(port);
		}
	}

	public int getClientMaxIdleSeconds() {
		return 3600;
	}

	public int getShutdownRequestPort() {
		String port = System.getProperty("brokerShutdownPort");
		if (!StringUtils.isNumeric(port)) {
			return DEFAULT_SHUTDOWN_PORT;
		} else {
			return Integer.valueOf(port);
		}
	}

	public int getMessageOffsetQueryPrecisionMillis() {
		return 30000;
	}

	public int getFetchMessageWithOffsetBatchSize() {
		return 500;
	}

	public int getAckMessagesTaskQueueSize() {
		return 500000;
	}

	public int getAckMessagesTaskExecutorThreadCount() {
		return 10;
	}

	public long getAckMessagesTaskExecutorCheckIntervalMillis() {
		return 100;
	}

	public MySQLCacheConfigProvider getMySQLCacheConfig() {
		return m_cacheConfig;
	}

	public int getFilterPatternCacheSize() {
		return m_filterPatternCacheSize;
	}

	public int getFilterPerTopicCacheSize() {
		return m_filterPerTopicCacheSize;
	}

	public int getFilterTopicCacheSize() {
		return m_filterTopicCacheSize;
	}

	public int getPullMessageSelectorWriteOffsetTtlMillis() {
		return m_pullMessageSelectorWriteOffsetTtlMillis;
	}

	public int getPullMessageSelectorSafeTriggerIntervalMillis() {
		return m_pullMessageSelectorSafeTriggerIntervalMillis;
	}

	public int getPullMessageSelectorOffsetLoaderThreadPoolSize() {
		return m_pullMessageSelectorOffsetLoaderThreadPoolSize;
	}

	public int getPullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds() {
		return m_pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds;
	}

	public long getPullMessageSelectorSafeTriggerMinFireIntervalMillis() {
		return m_pullMessageSelectorSafeTriggerMinFireIntervalMillis;
	}

	public int getSendMessageSelectorSafeTriggerMinFireIntervalMillis() {
		return m_sendMessageSelectorSafeTriggerMinFireIntervalMillis;
	}

	public int getSendMessageSelectorSafeTriggerIntervalMillis() {
		return m_sendMessageSelectorSafeTriggerIntervalMillis;
	}

	public int getSendMessageSelectorNormalTriggeringOffsetDelta(String topic) {
		Integer delta = m_sendMessageSelectorNormalTriggeringOffsetDeltas.get().get(topic);
		if (delta != null && delta > 0) {
			return delta;
		} else {
			return DEFAULT_SEND_MESSAGE_SELECTOR_NORMAL_TRIGGER_TRIGGERING_OFFSET_DELTA;
		}
	}

	public long getSendMessageSelectorSafeTriggerTriggeringOffsetDelta(String topic) {
		Integer delta = m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas.get().get(topic);
		if (delta != null && delta > 0) {
			return delta;
		} else {
			return DEFAULT_SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA;
		}
	}

	public int getPullMessageSelectorNormalTriggeringOffsetDelta(String topic, String groupId) {
		Map<String, Integer> topicDeltas = m_pullMessageSelectorNormalTriggeringOffsetDeltas.get().get(topic);
		if (topicDeltas == null) {
			return DEFAULT_PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTA;
		} else {
			Integer groupDelta = topicDeltas.get(groupId);
			if (groupDelta != null && groupDelta > 0) {
				return groupDelta;
			} else {
				groupDelta = topicDeltas.get("default");
				if (groupDelta != null && groupDelta > 0) {
					return groupDelta;
				} else {
					return DEFAULT_PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTA;
				}
			}
		}
	}

	public long getPullMessageSelectorSafeTriggerTriggeringOffsetDelta(String topic, String groupId) {
		Map<String, Integer> topicDeltas = m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas.get().get(topic);
		if (topicDeltas == null) {
			return DEFAULT_PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA;
		} else {
			Integer groupDelta = topicDeltas.get(groupId);
			if (groupDelta != null && groupDelta > 0) {
				return groupDelta;
			} else {
				groupDelta = topicDeltas.get("default");
				if (groupDelta != null && groupDelta > 0) {
					return groupDelta;
				} else {
					return DEFAULT_PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA;
				}
			}
		}
	}

	public int getMessageQueueFlushThreadCount() {
		return m_messageQueueFlushThreadCount;
	}

	public long getMessageQueueFetchPriorityMessageBySafeTriggerMinInterval() {
		return m_messageQueueFetchPriorityMessageBySafeTriggerMinInterval;
	}

	public long getMessageQueueFetchNonPriorityMessageBySafeTriggerMinInterval() {
		return m_messageQueueFetchNonPriorityMessageBySafeTriggerMinInterval;
	}

	public long getMessageQueueFetchResendMessageBySafeTriggerMinInterval() {
		return m_messageQueueFetchResendMessageBySafeTriggerMinInterval;
	}

	public RateLimiter getPartitionProduceQPSRateLimiter(String topic, int partition) {
		Double limit = getLimit(m_topicQPSRateLimits.get(), topic);

		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		RateLimiter rateLimiter = m_topicPartitionQPSRateLimiters.get(tp);

		if (rateLimiter == null) {
			synchronized (m_topicPartitionQPSRateLimiters) {
				rateLimiter = m_topicPartitionQPSRateLimiters.get(tp);
				if (rateLimiter == null) {
					rateLimiter = RateLimiter.create(limit);
					m_topicPartitionQPSRateLimiters.put(tp, rateLimiter);
					log.info("Set single partition's qps rate limit to {} for topic {} and partition {}", limit, topic,
					      partition);
				}
			}
		} else {
			synchronized (rateLimiter) {
				if (rateLimiter.getRate() != limit) {
					rateLimiter.setRate(limit);
					log.info("Single partition's qps rate limit changed to {} for topic {} and partition {}", limit, topic,
					      partition);
				}
			}
		}
		return rateLimiter;
	}

	public RateLimiter getPartitionProduceBytesRateLimiter(String topic, int partition) {
		Double limit = getLimit(m_topicBytesRateLimits.get(), topic);

		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		RateLimiter rateLimiter = m_topicPartitionBytesRateLimiters.get(tp);

		if (rateLimiter == null) {
			synchronized (m_topicPartitionBytesRateLimiters) {
				rateLimiter = m_topicPartitionBytesRateLimiters.get(tp);
				if (rateLimiter == null) {
					rateLimiter = RateLimiter.create(limit);
					m_topicPartitionBytesRateLimiters.put(tp, rateLimiter);
					log.info("Set single partition's bytes rate limit to {} for topic {}", limit, topic);
				}
			}
		} else {
			synchronized (rateLimiter) {
				if (rateLimiter.getRate() != limit) {
					rateLimiter.setRate(limit);
					log.info("Single partition's bytes rate limit changed to {} for topic {}", limit, topic);
				}
			}
		}
		return rateLimiter;
	}

	private Double getLimit(Map<String, Double> limits, String topic) {
		Double limit = limits.get(topic);
		if (limit == null) {
			limit = limits.get("default");

			if (limit == null) {
				return Double.MAX_VALUE;
			} else {
				return limit;
			}
		} else {
			return limit;
		}
	}

}
