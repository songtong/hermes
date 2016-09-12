package com.ctrip.hermes.broker.config;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerConfig.class)
public class BrokerConfig implements Initializable {
	@Inject
	private ClientEnvironment m_env;

	private String m_sessionId = UUID.randomUUID().toString();

	private long m_leaseRenewTimeMillsBeforeExpire = 2 * 1000L;

	private int m_longPollingServiceThreadCount = 50;

	private static final int DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE = 5000;

	private static final int DEFAULT_MYSQL_BATCH_INSERT_SIZE = 200;

	private int m_messageQueueFlushBatchSzie = DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE;

	private int m_mySQLBatchInsertSzie = DEFAULT_MYSQL_BATCH_INSERT_SIZE;

	private static final int DEFAULT_BROKER_PORT = 4376;

	private static final int DEFAULT_SHUTDOWN_PORT = 4888;

	private MySQLCacheConfig m_cacheConfig = new MySQLCacheConfig();

	private static final int DEFAULT_FILTER_PATTERN_CACHE_SIZE = 10000;

	private int m_filterPatternCacheSize = DEFAULT_FILTER_PATTERN_CACHE_SIZE;

	private static final int DEFAULT_FILTER_PER_TOPIC_CACHE_SIZE = 20;

	private int m_filterPerTopicCacheSize = DEFAULT_FILTER_PER_TOPIC_CACHE_SIZE;

	private static final int DEFAULT_FILTER_TOPIC_CACHE_SIZE = 5000;

	private static final int DEFAULT_SEND_MESSAGE_SELECTOR_NORMAL_TRIGGER_TRIGGERING_OFFSET_DELTA = 2;

	private static final int DEFAULT_SEND_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA = 1;

	private static final int DEFAULT_PULL_MESSAGE_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTA = 1;

	private static final int DEFAULT_PULL_MESSAGE_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA = 1;

	private int m_filterTopicCacheSize = DEFAULT_FILTER_TOPIC_CACHE_SIZE;

	private Map<String, Integer> m_sendMessageSelectorNormalTriggeringOffsetDeltas = new HashMap<>();

	private Map<String, Integer> m_sendMessageSelectorSafeTriggerTriggeringOffsetDeltas = new HashMap<>();

	private int m_pullMessageSelectorWriteOffsetTtlMillis = 8000;

	private int m_pullMessageSelectorSafeTriggerIntervalMillis = 500;

	private int m_pullMessageSelectorOffsetLoaderThreadPoolSize = 50;

	private int m_pullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds = 60;

	private int m_pullMessageSelectorSafeTriggerMinFireIntervalMillis = 500;

	private int m_sendMessageSelectorSafeTriggerMinFireIntervalMillis = 10;

	private int m_sendMessageSelectorSafeTriggerIntervalMillis = 10;

	// Topic -> GroupId or default -> delta
	private Map<String, Map<String, Integer>> m_pullMessageSelectorNormalTriggeringOffsetDeltas = new HashMap<>();

	private Map<String, Map<String, Integer>> m_pullMessageSelectorSafeTriggerTriggeringOffsetDeltas = new HashMap<>();

	private int m_messageQueueFlushThreadCount = 500;

	private long m_messageQueueFetchPriorityMessageMinInterval = 100;

	private long m_messageQueueFetchNonPriorityMessageMinInterval = 100;

	private long m_messageQueueFetchResendMessageMinInterval = 100;

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
		String messageQueueFetchPriorityMessageMinIntervalStr = m_env.getGlobalConfig().getProperty("broker.mq.priroty.msg.min.fetch.interval");
		if (StringUtils.isNumeric(messageQueueFetchPriorityMessageMinIntervalStr)) {
			m_messageQueueFetchPriorityMessageMinInterval = Integer.valueOf(messageQueueFetchPriorityMessageMinIntervalStr);
		}
		String messageQueueFetchNonPriorityMessageMinIntervalStr = m_env.getGlobalConfig().getProperty("broker.mq.nonpriroty.msg.min.fetch.interval");
		if (StringUtils.isNumeric(messageQueueFetchNonPriorityMessageMinIntervalStr)) {
			m_messageQueueFetchNonPriorityMessageMinInterval = Integer.valueOf(messageQueueFetchNonPriorityMessageMinIntervalStr);
		}
		String messageQueueFetchResendMessageMinIntervalStr = m_env.getGlobalConfig().getProperty("broker.mq.resend.msg.min.fetch.interval");
		if (StringUtils.isNumeric(messageQueueFetchResendMessageMinIntervalStr)) {
			m_messageQueueFetchResendMessageMinInterval = Integer.valueOf(messageQueueFetchResendMessageMinIntervalStr);
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

	public MySQLCacheConfig getMySQLCacheConfig() {
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

	public long getMessageQueueFetchPriorityMessageMinInterval() {
		return m_messageQueueFetchPriorityMessageMinInterval;
	}

	public long getMessageQueueFetchNonPriorityMessageMinInterval() {
		return m_messageQueueFetchNonPriorityMessageMinInterval;
	}

	public long getMessageQueueFetchResendMessageMinInterval() {
		return m_messageQueueFetchResendMessageMinInterval;
	}

}
