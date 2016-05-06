package com.ctrip.hermes.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.MessageStream;
import com.ctrip.hermes.consumer.api.MessageStreamOffset;
import com.ctrip.hermes.consumer.api.OffsetStorage;
import com.ctrip.hermes.consumer.api.PartitionMetaListener;
import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.api.PullConsumerHolder;
import com.ctrip.hermes.consumer.engine.CompositeSubscribeHandle;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.pull.DefaultPullConsumerHolder;
import com.ctrip.hermes.consumer.stream.DefaultMessageStream;
import com.ctrip.hermes.consumer.stream.DefaultMessageStreamHolder;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = com.ctrip.hermes.consumer.api.Consumer.class)
public class DefaultConsumer extends Consumer {
	private static final Logger log = LoggerFactory.getLogger(DefaultConsumer.class);

	private static final MessageListenerConfig DEFAULT_MESSAGE_LISTENER_CONFIG = new MessageListenerConfig();

	@Inject
	private Engine m_engine;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ConsumerConfig m_config;

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType) {
		return start(topicPattern, groupId, listener, listenerConfig, consumerType, null);
	}

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType, OffsetStorage offsetStorage) {
		return start(topicPattern, groupId, listener, listenerConfig, consumerType, offsetStorage, null);
	}

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType, OffsetStorage offsetStorage,
	      Class<?> messageClazz) {
		if (listener instanceof BaseMessageListener) {
			((BaseMessageListener<?>) listener).setGroupId(groupId);
		}
		SubscribeHandle subscribeHandle = m_engine.start(new Subscriber(topicPattern, groupId, listener, listenerConfig,
		      consumerType, offsetStorage, messageClazz));

		return new DefaultConsumerHolder(subscribeHandle);
	}

	@Override
	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener) {
		return start(topic, groupId, listener, DEFAULT_MESSAGE_LISTENER_CONFIG);
	}

	@Override
	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener, MessageListenerConfig config) {
		ConsumerType type = config.isStrictlyOrdering() ? ConsumerType.STRICTLY_ORDERING : ConsumerType.DEFAULT;
		return start(topic, groupId, listener, config, type);
	}

	public static class DefaultConsumerHolder implements ConsumerHolder {

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
	public MessageStreamOffset getOffsetByTime(String topicName, int id, long time) {
		Topic topic = m_metaService.findTopicByName(topicName);
		if (topic == null || topic.findPartition(id) == null) {
			throw new IllegalArgumentException(String.format("Topic [%s] or partition [%s] not found.", topicName, id));
		}
		return new MessageStreamOffset(m_metaService.findMessageOffsetByTime(topicName, id, time));
	}

	@Override
	public Map<Integer, MessageStreamOffset> getOffsetByTime(String topicName, long time) {
		Topic topic = m_metaService.findTopicByName(topicName);
		if (topic == null) {
			throw new IllegalArgumentException(String.format("Topic [%s] not found.", topicName));
		}

		Map<Integer, MessageStreamOffset> offsets = new HashMap<>();
		Map<Integer, Offset> map = m_metaService.findMessageOffsetByTime(topicName, time);
		if (map != null) {
			for (Entry<Integer, Offset> entry : map.entrySet()) {
				offsets.put(entry.getKey(), new MessageStreamOffset(entry.getValue()));
			}
		}
		return offsets;
	}

	@Override
	public <T> MessageStreamHolder<T> openMessageStreams(String topicName, String groupId, Class<T> messageClass,
	      OffsetStorage offsetStorage, PartitionMetaListener partitionListener) {
		Topic topic = validateTopicAndConsumerGroup(topicName, groupId);

		StreamMessageListener<T> listener = new StreamMessageListener<T>();
		List<MessageStream<T>> streams = new ArrayList<>();
		for (Partition p : topic.getPartitions()) {
			DefaultMessageStream<T> stream = new DefaultMessageStream<T>(new Tpg(topicName, p.getId(), groupId),
			      messageClass);
			streams.add(stream);
			listener.registerMessageStream(stream);
		}

		ConsumerHolder consumerHolder = start(topicName, groupId, listener, DEFAULT_MESSAGE_LISTENER_CONFIG,
		      ConsumerType.MESSAGE_STREAM, offsetStorage, messageClass);

		DefaultMessageStreamHolder<T> streamHolder = new DefaultMessageStreamHolder<T>( //
		      topicName, groupId, consumerHolder, streams);

		streamHolder.startPartitionWatchdog(partitionListener, m_config.getPartitionWatchdogIntervalSeconds());

		return streamHolder;
	}

	private Topic validateTopicAndConsumerGroup(String topicName, String groupId) {
		Topic topic = m_metaService.findTopicByName(topicName);

		if (topic == null) {
			throw new IllegalArgumentException("Topic not found: " + topicName);
		}
		if (topic.findConsumerGroup(groupId) == null) {
			throw new IllegalArgumentException("Consumer group not found: " + groupId);
		}
		return topic;
	}

	private static class StreamMessageListener<T> extends BaseMessageListener<T> {
		private Map<Integer, BlockingQueue<ConsumerMessage<T>>> m_queues = new HashMap<>();

		public void registerMessageStream(DefaultMessageStream<T> stream) {
			m_queues.put(stream.getParatitionId(), stream.getQueue());
		}

		@Override
		protected void onMessage(ConsumerMessage<T> msg) {
			try {
				int partitionId = msg.getPartition();
				m_queues.get(partitionId).put(msg);
			} catch (InterruptedException e) {
				log.error("Put message to stream cache failed.", e);
			}
		}
	}

	@Override
	public <T> PullConsumerHolder<T> openPullConsumer(String topicName, String groupId, Class<T> messageClass,
	      PullConsumerConfig config) {
		ensureSingleTopic(topicName);

		Topic topic = validateTopicAndConsumerGroup(topicName, groupId);

		DefaultPullConsumerHolder<T> holder = new DefaultPullConsumerHolder<T>(topicName, groupId, topic.getPartitions()
		      .size(), config, PlexusComponentLocator.lookup(AckManager.class), m_config);

		MessageListenerConfig listenerConfig = new MessageListenerConfig();
		ConsumerHolder consumerHolder = start(topicName, groupId, holder, listenerConfig, ConsumerType.PULL, null,
		      messageClass);
		holder.setConsumerHolder(consumerHolder);

		return holder;
	}

	private void ensureSingleTopic(String topicName) {
		if (topicName != null && (topicName.indexOf("*") >= 0 || topicName.indexOf("#") >= 0)) {
			throw new IllegalArgumentException("Pull consumer can not use topic pattern: " + topicName);
		}

	}
}
