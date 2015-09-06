package com.ctrip.hermes.broker.queue;

import java.util.concurrent.ScheduledExecutorService;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Storage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageQueuePartitionFactory.class)
public class MessageQueuePartitionFactory extends ContainerHolder {
	@Inject
	private MetaService m_metaService;

	@Inject
	private BrokerConfig m_config;

	public MessageQueue getMessageQueue(String topic, int partition, ScheduledExecutorService ackOpExecutor) {
		Storage storage = m_metaService.findStorageByTopic(topic);
		try {
			return new DefaultMessageQueue(topic, partition, lookup(MessageQueueStorage.class, storage.getType()),
			      m_metaService, m_config, ackOpExecutor);
		} catch (Exception e) {
			throw new IllegalArgumentException("Unsupported storage type " + storage.getType(), e);
		}
	}
}
