package com.ctrip.hermes.consumer.engine;

import java.util.UUID;

import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SuppressWarnings("rawtypes")
public class ConsumerContext {
	private Topic m_topic;

	private ConsumerGroup m_group;

	private String m_auditName;

	private Class<?> m_messageClazz;

	private MessageListener m_consumer;

	private ConsumerType m_consumerType;

	private MessageListenerConfig m_messageListenerConfig;

	private String m_sessionId = UUID.randomUUID().toString();

	public ConsumerContext(Topic topic, ConsumerGroup group, MessageListener consumer, Class<?> messageClazz,
	      ConsumerType consumerType, MessageListenerConfig messageListenerConfig) {
		m_topic = topic;
		m_group = group;
		m_auditName = topic.getName() + "_" + group.getAppIds();
		m_consumer = consumer;
		m_messageClazz = messageClazz;
		m_consumerType = consumerType;
		m_messageListenerConfig = messageListenerConfig;
	}

	public String getSessionId() {
		return m_sessionId;
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

	public ConsumerGroup getGroup() {
		return m_group;
	}

	public String getGroupId() {
		return m_group.getName();
	}

	public String getAuditAppName(){
		return m_auditName;
	}
	
	public MessageListener getConsumer() {
		return m_consumer;
	}

	public MessageListenerConfig getMessageListenerConfig() {
		return m_messageListenerConfig;
	}

	@Override
	public String toString() {
		return "ConsumerContext [m_topic=" + m_topic.getName() + ", m_groupId=" + m_group.getName() + ", m_messageClazz="
		      + m_messageClazz + ", m_consumer=" + m_consumer + ", m_consumerType=" + m_consumerType + "]";
	}

}
