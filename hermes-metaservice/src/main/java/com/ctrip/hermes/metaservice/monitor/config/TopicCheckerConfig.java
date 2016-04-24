package com.ctrip.hermes.metaservice.monitor.config;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;

public class TopicCheckerConfig {

	public static final int DFT_TOPIC_LARGE_DEADLETTER_LIMIT = 0;

	public static final int DFT_LONG_TIME_NO_PRODUCE_LIMIT = 30;

	private String m_topic;

	private boolean m_topicLargeDeadLetterSwitch;

	private int m_topicLargeDeadLetterLimit = DFT_TOPIC_LARGE_DEADLETTER_LIMIT;

	private boolean m_longTimeNoProduceSwitch;

	private int m_longTimeNoProduceLimit = DFT_LONG_TIME_NO_PRODUCE_LIMIT;

	private Map<String, ConsumerCheckerConfig> m_consumerCheckerConfigs = new HashMap<>();

	public static TopicCheckerConfig fromTopic(Topic topic) {
		if (topic == null) {
			return null;
		}
		TopicCheckerConfig config = new TopicCheckerConfig();
		config.setTopic(topic.getName());
		for (ConsumerGroup consumer : topic.getConsumerGroups()) {
			config.getConsumerCheckerConfigs().put(consumer.getName(), new ConsumerCheckerConfig());
		}
		return config;
	}

	public static class ConsumerCheckerConfig {
		public static final int DFT_CONSUME_LARGE_BACKLOG_LIMIT = 10000;

		public static final int DFT_CONSUME_LARGE_DELAY_LIMIT = 5000;

		public static final int DFT_LONG_TIME_NO_CONSUME_LIMIT = 30;

		private String m_consumer;

		private boolean m_consumeLargeBacklogSwitch;

		private int m_consumeLargeBacklogLimit = DFT_CONSUME_LARGE_BACKLOG_LIMIT;

		private boolean m_consumeLargeDelaySwitch;

		private int m_consumeLargeDelayLimit = DFT_CONSUME_LARGE_DELAY_LIMIT;

		private boolean m_longTimeNoConsumeSwitch;

		private int m_longTimeNoConsumeLimit = DFT_LONG_TIME_NO_CONSUME_LIMIT;

		public String getConsumer() {
			return m_consumer;
		}

		public void setConsumer(String consumer) {
			m_consumer = consumer;
		}

		public boolean isConsumeLargeBacklogSwitch() {
			return m_consumeLargeBacklogSwitch;
		}

		public void setConsumeLargeBacklogSwitch(boolean consumeLargeBacklogSwitch) {
			m_consumeLargeBacklogSwitch = consumeLargeBacklogSwitch;
		}

		public int getConsumeLargeBacklogLimit() {
			return m_consumeLargeBacklogLimit;
		}

		public void setConsumeLargeBacklogLimit(int consumeLargeBacklogLimit) {
			m_consumeLargeBacklogLimit = consumeLargeBacklogLimit;
			setConsumeLargeBacklogSwitch(true);
		}

		public boolean isConsumeLargeDelaySwitch() {
			return m_consumeLargeDelaySwitch;
		}

		public void setConsumeLargeDelaySwitch(boolean consumeLargeDelaySwitch) {
			m_consumeLargeDelaySwitch = consumeLargeDelaySwitch;
		}

		public int getConsumeLargeDelayLimit() {
			return m_consumeLargeDelayLimit;
		}

		public void setConsumeLargeDelayLimit(int consumeLargeDelayLimit) {
			m_consumeLargeDelayLimit = consumeLargeDelayLimit;
			setConsumeLargeDelaySwitch(true);
		}

		public boolean isLongTimeNoConsumeSwitch() {
			return m_longTimeNoConsumeSwitch;
		}

		public void setLongTimeNoConsumeSwitch(boolean longTimeNoConsumeSwitch) {
			m_longTimeNoConsumeSwitch = longTimeNoConsumeSwitch;
		}

		public int getLongTimeNoConsumeLimit() {
			return m_longTimeNoConsumeLimit;
		}

		public void setLongTimeNoConsumeLimit(int longTimeNoConsumeLimit) {
			m_longTimeNoConsumeLimit = longTimeNoConsumeLimit;
			setLongTimeNoConsumeSwitch(true);
		}

		@Override
		public String toString() {
			return "ConsumerCheckerConfig [m_consumer=" + m_consumer + ", m_consumeLargeBacklogSwitch="
			      + m_consumeLargeBacklogSwitch + ", m_consumeLargeBacklogLimit=" + m_consumeLargeBacklogLimit
			      + ", m_consumeLargeDelaySwitch=" + m_consumeLargeDelaySwitch + ", m_consumeLargeDelayLimit="
			      + m_consumeLargeDelayLimit + ", m_longTimeNoConsumeSwitch=" + m_longTimeNoConsumeSwitch
			      + ", m_longTimeNoConsumeLimit=" + m_longTimeNoConsumeLimit + "]";
		}
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public boolean isTopicLargeDeadLetterSwitch() {
		return m_topicLargeDeadLetterSwitch;
	}

	public void setTopicLargeDeadLetterSwitch(boolean topicLargeDeadLetterSwitch) {
		m_topicLargeDeadLetterSwitch = topicLargeDeadLetterSwitch;
	}

	public int getTopicLargeDeadLetterLimit() {
		return m_topicLargeDeadLetterLimit;
	}

	public void setTopicLargeDeadLetterLimit(int topicLargeDeadLetterLimit) {
		m_topicLargeDeadLetterLimit = topicLargeDeadLetterLimit;
		setTopicLargeDeadLetterSwitch(true);
	}

	public boolean isLongTimeNoProduceSwitch() {
		return m_longTimeNoProduceSwitch;
	}

	public void setLongTimeNoProduceSwitch(boolean longTimeNoProduceSwitch) {
		m_longTimeNoProduceSwitch = longTimeNoProduceSwitch;
	}

	public int getLongTimeNoProduceLimit() {
		return m_longTimeNoProduceLimit;
	}

	public void setLongTimeNoProduceLimit(int longTimeNoProduceLimit) {
		m_longTimeNoProduceLimit = longTimeNoProduceLimit;
		setLongTimeNoProduceSwitch(true);
	}

	public Map<String, ConsumerCheckerConfig> getConsumerCheckerConfigs() {
		return m_consumerCheckerConfigs;
	}

	public void setConsumerCheckerConfigs(Map<String, ConsumerCheckerConfig> consumerMonitorConfigs) {
		m_consumerCheckerConfigs = consumerMonitorConfigs;
	}

	public ConsumerCheckerConfig findConsumerCheckerConfig(String consumer) {
		return m_consumerCheckerConfigs.get(consumer);
	}

	@Override
	public String toString() {
		return "TopicCheckerConfig [m_topic=" + m_topic + ", m_topicLargeDeadLetterSwitch="
		      + m_topicLargeDeadLetterSwitch + ", m_topicLargeDeadLetterLimit=" + m_topicLargeDeadLetterLimit
		      + ", m_longTimeNoProduceSwitch=" + m_longTimeNoProduceSwitch + ", m_longTimeNoProduceLimit="
		      + m_longTimeNoProduceLimit + ", m_consumerCheckerConfigs=" + m_consumerCheckerConfigs + "]";
	}
}
