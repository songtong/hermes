package com.ctrip.hermes.broker.queue.kafka;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.AbstractMessageQueue;
import com.ctrip.hermes.broker.queue.MessageQueueCursor;
import com.ctrip.hermes.broker.queue.MessageQueueDumper;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;

public class KafkaMessageQueue extends AbstractMessageQueue {

	private BrokerConfig m_config;

	private MetaService m_metaService;

	public KafkaMessageQueue(String topic, int partition, MessageQueueStorage storage, MetaService metaService,
	      BrokerConfig m_config) {
		super(topic, partition, storage);
		this.m_config = m_config;
		this.m_metaService = metaService;
	}

	@Override
	protected MessageQueueDumper createDumper(Lease lease) {
		return new KafkaMessageQueueDumper(m_topic, m_partition, m_config, lease, m_storage, m_metaService);
	}

	@Override
	protected MessageQueueCursor create(String groupId, Lease lease) {
		return null;
	}

	@Override
	protected void doNack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas) {

	}

	@Override
	protected void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq) {

	}

}
