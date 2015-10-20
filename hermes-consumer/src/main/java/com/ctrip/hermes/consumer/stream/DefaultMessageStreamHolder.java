package com.ctrip.hermes.consumer.stream;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.Consumer.MessageStreamHolder;
import com.ctrip.hermes.consumer.api.MessageStream;
import com.ctrip.hermes.consumer.api.PartitionMetaListener;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Topic;

public class DefaultMessageStreamHolder<T> implements MessageStreamHolder<T> {
	private static final Logger log = LoggerFactory.getLogger(DefaultMessageStreamHolder.class);

	private List<MessageStream<T>> m_streams;

	private ConsumerHolder m_consumer;

	private String m_topic;

	private String m_group;

	private MetaService m_metaService = PlexusComponentLocator.lookup(MetaService.class);

	private int m_lastValue = -1;

	private ExecutorService m_watchdog;

	public DefaultMessageStreamHolder(String topic, String group, ConsumerHolder consumer, List<MessageStream<T>> streams) {
		m_topic = topic;
		m_streams = streams;
		m_consumer = consumer;
		m_group = group;
		m_lastValue = streams.size();
	}

	@Override
	public String getTopic() {
		return m_topic;
	}

	@Override
	public String getConsumerGroup() {
		return m_group;
	}

	@Override
	public List<MessageStream<T>> getStreams() {
		return m_streams;
	}

	@Override
	public void close() {
		m_consumer.close();
		m_watchdog.shutdown();
	}

	public void startPartitionWatchdog(final PartitionMetaListener listener, long intervalInSeconds) {
		m_watchdog = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("PARTITION_WATCHDOG", true));
		((ScheduledExecutorService) m_watchdog).scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				try {
					Topic topic = m_metaService.findTopicByName(m_topic);
					if (m_lastValue != -1 && topic != null && topic.getPartitions().size() != m_lastValue) {
						try {
							listener.onPartitionCountChanged(m_topic);
							m_lastValue = topic.getPartitions().size();
						} catch (Exception e) {
							log.error("Notify PartitionMetaListener failed.", e);
						}
					}
				} catch (Exception e) {
					log.error("Error happens when refresh partition info. [{}]", m_topic, e);
				}
			}
		}, intervalInSeconds, intervalInSeconds, TimeUnit.SECONDS);
	}
}
