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

	private static final String RATE_LIMITS_QPS = "rateLimits.qps";

	private static final String RATE_LIMITS_BYTES = "rateLimits.bytes";

	@Inject
	private ClientEnvironment m_env;

	private String m_sessionId;

	private long m_leaseRenewTimeMillsBeforeExpire = 2 * 1000L;

	private int m_longPollingServiceThreadCount = 50;

	private static final int DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE = 5000;

	private static final int DEFAULT_MYSQL_BATCH_INSERT_SIZE = 200;

	private int m_messageQueueFlushBatchSzie = DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE;

	private int m_mySQLBatchInsertSzie = DEFAULT_MYSQL_BATCH_INSERT_SIZE;

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

	private int m_filterTopicCacheSize = DEFAULT_FILTER_TOPIC_CACHE_SIZE;

	private Map<String, Integer> m_sendMessageSelectorNormalTriggeringOffsetDeltas = new HashMap<>();

	private Map<String, Integer> m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = new HashMap<>();

	private int m_pullMessageSelectorWriteOffsetTtlMillis = 8000;

	private int m_pullMessageSelectorSafeTriggerIntervalMillis = 1000;

	private int m_pullMessageSelectorOffsetLoaderThreadPoolSize = 50;

	private int m_pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds = 60;

	private int m_pullMessageSelectorSafeTriggerMinFireIntervalMillis = 1000;

	private int m_sendMessageSelectorSafeTriggerMinFireIntervalMillis = 10;

	private int m_sendMessageSelectorSafeTriggerIntervalMillis = 10;

	// Topic -> GroupId or default -> delta
	private Map<String, Map<String, Integer>> m_pullMessageSelectorNormalTriggeringOffsetDeltas = new HashMap<>();

	private Map<String, Map<String, Integer>> m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = new HashMap<>();

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

		String mysqlBatchInsertSizeStr = m_env.getGlobalConfig().getProperty("broker.mysql.batch.size");
		if (StringUtils.isNumeric(mysqlBatchInsertSizeStr)) {
			m_mySQLBatchInsertSzie = Integer.valueOf(mysqlBatchInsertSizeStr);
		}

		String longPollingServiceThreadCount = m_env.getGlobalConfig().getProperty(
		      "broker.long.polling.service.thread.count");
		if (StringUtils.isNumeric(longPollingServiceThreadCount)) {
			m_longPollingServiceThreadCount = Integer.valueOf(longPollingServiceThreadCount);
		}

		// pull message selector
		String pullMessageSelectorNormalTriggeringOffsetDeltas = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.normal.triggering.offset.deltas");
		if (!StringUtils.isEmpty(pullMessageSelectorNormalTriggeringOffsetDeltas)) {
			m_pullMessageSelectorNormalTriggeringOffsetDeltas = JSON.parseObject(
			      pullMessageSelectorNormalTriggeringOffsetDeltas, new TypeReference<Map<String, Map<String, Integer>>>() {
			      });
		}

		String pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.safe.trigger.triggering.offset.deltas");
		if (!StringUtils.isEmpty(pullMessageSelectorSafeTriggerTriggeringOffsetDeltas)) {
			m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = JSON.parseObject(
			      pullMessageSelectorSafeTriggerTriggeringOffsetDeltas,
			      new TypeReference<Map<String, Map<String, Integer>>>() {
			      });
		}

		String pullMessageSelectorWriteOffsetTtlMillis = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.write.offset.ttl.millis");
		if (StringUtils.isNumeric(pullMessageSelectorWriteOffsetTtlMillis)) {
			m_pullMessageSelectorWriteOffsetTtlMillis = Integer.valueOf(pullMessageSelectorWriteOffsetTtlMillis);
		}

		String pullMessageSelectorSafeTriggerIntervalMillis = m_env.getGlobalConfig().getProperty(
		      "broker.pull.message.selector.safe.trigger.interval.millis");
		if (StringUtils.isNumeric(pullMessageSelectorSafeTriggerIntervalMillis)) {
			m_pullMessageSelectorSafeTriggerIntervalMillis = Integer.valueOf(pullMessageSelectorSafeTriggerIntervalMillis);
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

		// send message selector
		String sendMessageSelectorNormalTriggeringOffsetDeltas = m_env.getGlobalConfig().getProperty(
		      "broker.send.message.selector.normal.triggering.offset.deltas");
		if (!StringUtils.isEmpty(sendMessageSelectorNormalTriggeringOffsetDeltas)) {
			m_sendMessageSelectorNormalTriggeringOffsetDeltas = JSON.parseObject(
			      sendMessageSelectorNormalTriggeringOffsetDeltas, new TypeReference<Map<String, Integer>>() {
			      });
		}

		String sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = m_env.getGlobalConfig().getProperty(
		      "broker.send.message.selector.safe.trigger.triggering.offset.deltas");
		if (!StringUtils.isEmpty(sendMessageSelectorSafeTriggerTriggeringOffsetDeltas)) {
			m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = JSON.parseObject(
			      sendMessageSelectorSafeTriggerTriggeringOffsetDeltas, new TypeReference<Map<String, Integer>>() {
			      });
		}

		String sendMessageSelectorSafeTriggerMinFireIntervalMillis = m_env.getGlobalConfig().getProperty(
		      "broker.send.message.selector.safe.trigger.min.fire.interval.millis");
		if (StringUtils.isNumeric(sendMessageSelectorSafeTriggerMinFireIntervalMillis)) {
			m_sendMessageSelectorSafeTriggerMinFireIntervalMillis = Integer
			      .valueOf(sendMessageSelectorSafeTriggerMinFireIntervalMillis);
		}

		String sendMessageSelectorSafeTriggerIntervalMillis = m_env.getGlobalConfig().getProperty(
		      "broker.send.message.selector.safe.trigger.interval.millis");
		if (StringUtils.isNumeric(sendMessageSelectorSafeTriggerIntervalMillis)) {
			m_sendMessageSelectorSafeTriggerIntervalMillis = Integer.valueOf(sendMessageSelectorSafeTriggerIntervalMillis);
		}

		m_cacheConfig.init(m_env.getGlobalConfig());

		initRateLimiters();
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

	public int getMySQLBatchInsertSize() {
		return m_mySQLBatchInsertSzie;
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
		Integer delta = m_sendMessageSelectorNormalTriggeringOffsetDeltas.get(topic);
		if (delta != null && delta > 0) {
			return delta;
		} else {
			return DEFAULT_SEND_MESSAGE_SELECTOR_NORMAL_TRIGGER_TRIGGERING_OFFSET_DELTA;
		}
	}

	public long getSendMessageSelectorSafeTriggerTriggeringOffsetDelta(String topic) {
		Integer delta = m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas.get(topic);
		if (delta != null && delta > 0) {
			return delta;
		} else {
			return DEFAULT_SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA;
		}
	}

	public int getPullMessageSelectorNormalTriggeringOffsetDelta(String topic, String groupId) {
		Map<String, Integer> topicDeltas = m_pullMessageSelectorNormalTriggeringOffsetDeltas.get(topic);
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
		Map<String, Integer> topicDeltas = m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas.get(topic);
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
