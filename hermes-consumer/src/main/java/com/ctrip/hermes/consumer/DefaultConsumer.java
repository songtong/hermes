package com.ctrip.hermes.consumer;

import java.util.Arrays;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.Subscriber;

@Named(type = com.ctrip.hermes.consumer.api.Consumer.class)
public class DefaultConsumer extends com.ctrip.hermes.consumer.api.Consumer {

	@Inject
	private Engine m_engine;

	private ConsumerHolder start(String topic, String groupId, MessageListener<?> listener, ConsumerType consumerType) {
		SubscribeHandle subscribeHandle = m_engine.start(Arrays.asList(new Subscriber(topic, groupId, listener,
		      consumerType)));

		return new DefaultConsumerHolder(subscribeHandle);
	}

	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener) {
		return start(topic, groupId, listener, ConsumerType.LONG_POLLING);
	}

	public class DefaultConsumerHolder implements ConsumerHolder {

		private SubscribeHandle m_subscribeHandle;

		public DefaultConsumerHolder(SubscribeHandle subscribeHandle) {
			m_subscribeHandle = subscribeHandle;
		}

		@Override
		public void close() {
			m_subscribeHandle.close();
		}

	}
}
