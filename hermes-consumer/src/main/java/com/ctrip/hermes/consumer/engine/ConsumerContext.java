package com.ctrip.hermes.consumer.engine;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SuppressWarnings("rawtypes")
public class ConsumerContext {
	private Topic m_topic;

	private String m_groupId;

	private Class<?> m_messageClazz;

	private Consumer m_consumer;

	private ConsumerType m_consumerType;

	public ConsumerContext(Topic topic, String groupId, Consumer consumer, Class<?> messageClazz,
	      ConsumerType consumerType) {
		m_topic = topic;
		m_groupId = groupId;
		m_consumer = consumer;
		m_messageClazz = messageClazz;
		m_consumerType = consumerType;
	}

	public ConsumerType getConsumerType() {
		return m_consumerType;
	}

	public Class<?> getMessageClazz() {
		return m_messageClazz;
	}

	public Topic getTopic() {
		return m_topic;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public Consumer getConsumer() {
		return m_consumer;
	}

	@Override
	public String toString() {
		return "ConsumerContext [m_topic=" + m_topic.getName() + ", m_groupId=" + m_groupId + ", m_messageClazz=" + m_messageClazz
		      + ", m_consumer=" + m_consumer + ", m_consumerType=" + m_consumerType + "]";
	}

}
