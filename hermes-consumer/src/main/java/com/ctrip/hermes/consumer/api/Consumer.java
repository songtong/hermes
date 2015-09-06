package com.ctrip.hermes.consumer.api;

import java.util.List;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Consumer {

	public abstract ConsumerHolder start(String topic, String groupId, MessageListener<?> listener);
	
	public abstract ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener, MessageListenerConfig config);
	
	public abstract <T> List<MessageStream<T>> createMessageStreams(String topic, String groupId);


	public static Consumer getInstance() {
		return PlexusComponentLocator.lookup(Consumer.class);
	}

	public interface ConsumerHolder {
		void close();
	}

}
