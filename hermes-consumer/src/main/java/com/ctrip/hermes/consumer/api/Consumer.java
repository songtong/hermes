package com.ctrip.hermes.consumer.api;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Consumer {

	public abstract ConsumerHolder start(String topic, String groupId, MessageListener<?> listener);

	public static Consumer getInstance() {
		return PlexusComponentLocator.lookup(Consumer.class);
	}

	public interface ConsumerHolder {
		void close();
	}

}
