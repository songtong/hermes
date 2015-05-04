package com.ctrip.hermes.producer.api;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.result.Callback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Producer {

	/**
	 * 
	 * @param topic
	 * @param partitionKey
	 * @param body
	 * @return
	 */
	public abstract MessageHolder message(String topic, String partitionKey, Object body);
	
	public static Producer getInstance() {
		return PlexusComponentLocator.lookup(Producer.class);
	}

	public interface MessageHolder {
		public MessageHolder withRefKey(String key);

		public Future<SendResult> send();
		
		public MessageHolder withPriority();

		public MessageHolder addProperty(String key, String value);
		
		public MessageHolder setCallback(Callback callback);
	}
}
