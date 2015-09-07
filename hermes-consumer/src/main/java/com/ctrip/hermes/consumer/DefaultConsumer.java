package com.ctrip.hermes.consumer;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.MessageStream;
import com.ctrip.hermes.consumer.engine.CompositeSubscribeHandle;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.Subscriber;

@Named(type = com.ctrip.hermes.consumer.api.Consumer.class)
public class DefaultConsumer extends com.ctrip.hermes.consumer.api.Consumer {

	private final static MessageListenerConfig DEFAULT_MESSAGE_LISTENER_CONFIG = new MessageListenerConfig();

	@Inject
	private Engine m_engine;

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType) {
		if (listener instanceof BaseMessageListener) {
			((BaseMessageListener<?>) listener).setGroupId(groupId);
		}
		SubscribeHandle subscribeHandle = m_engine.start(new Subscriber(topicPattern, groupId, listener, listenerConfig,
		      consumerType));

		return new DefaultConsumerHolder(subscribeHandle);
	}

	@Override
	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener) {
		return start(topic, groupId, listener, DEFAULT_MESSAGE_LISTENER_CONFIG);
	}

	@Override
	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener, MessageListenerConfig config) {
		return start(topic, groupId, listener, config, config.isStrictlyOrdering() ? ConsumerType.STRICTLY_ORDERING
		      : ConsumerType.DEFAULT);
	}

	public class DefaultConsumerHolder implements ConsumerHolder {

		private SubscribeHandle m_subscribeHandle;

		public DefaultConsumerHolder(SubscribeHandle subscribeHandle) {
			m_subscribeHandle = subscribeHandle;
		}

		public boolean isConsuming() {
			if (m_subscribeHandle instanceof CompositeSubscribeHandle) {
				return ((CompositeSubscribeHandle) m_subscribeHandle).getChildHandleList().size() > 0;
			}
			return false;
		}

		@Override
		public void close() {
			m_subscribeHandle.close();
		}

	}

	@Override
	public <T> List<MessageStream<T>> createMessageStreams(String topic, String groupId) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}
