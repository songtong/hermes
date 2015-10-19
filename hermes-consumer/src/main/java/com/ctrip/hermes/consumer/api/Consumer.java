package com.ctrip.hermes.consumer.api;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Consumer {

	public abstract ConsumerHolder start(String topic, String groupId, MessageListener<?> listener);

	public abstract ConsumerHolder start( //
	      String topicPattern, String groupId, MessageListener<?> listener, MessageListenerConfig config);

	public abstract <T> MessageStreamHolder<T> openMessageStreams(String topic, String groupId, Class<T> messageClass,
	      OffsetStorage offsetStorage, PartitionMetaListener partitionListener);

	public abstract Map<Integer, MessageStreamOffset> getOffsetByTime(String topic, long time);

	public abstract MessageStreamOffset getOffsetByTime(String topic, int partitionId, long time);

	public static Consumer getInstance() {
		return PlexusComponentLocator.lookup(Consumer.class);
	}

	public interface ConsumerHolder {
		void close();
	}

	public interface MessageStreamHolder<T> {
		void close();

		public String getTopic();

		public String getConsumerGroup();

		public List<MessageStream<T>> getStreams();
	}

}
