package com.ctrip.hermes.consumer.engine;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.consumer.api.MessageListener;

@SuppressWarnings("rawtypes")
public class Subscriber {

	private String m_groupId;

	private String m_topicPattern;

	private MessageListener m_consumer;

	private ConsumerType m_consumerType;

	public Subscriber(String topicPattern, String groupId, MessageListener consumer, ConsumerType consumerType) {
		m_topicPattern = topicPattern;
		m_groupId = groupId;
		m_consumer = consumer;
		m_consumerType = consumerType;
	}

	public Subscriber(String topicPattern, String groupId, MessageListener consumer) {
		this(topicPattern, groupId, consumer, ConsumerType.LONG_POLLING);
	}

	public ConsumerType getConsumerType() {
		return m_consumerType;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopicPattern() {
		return m_topicPattern;
	}

	public MessageListener getConsumer() {
		return m_consumer;
	}

	public Class<?> getMessageClass() {
		Type genericSuperClass = m_consumer.getClass().getGenericSuperclass();
		if (genericSuperClass instanceof ParameterizedType) {
			ParameterizedType paraType = (ParameterizedType) genericSuperClass;
			Type[] actualTypeArguments = paraType.getActualTypeArguments();
			Class type = (Class) actualTypeArguments[0];
			return type;
		} else {
			Type[] genericInterfaces = m_consumer.getClass().getGenericInterfaces();
			Type[] actualTypeArguments = ((ParameterizedType) genericInterfaces[0]).getActualTypeArguments();
			Class type = (Class) actualTypeArguments[0];
			return type;
		}
	}
}
