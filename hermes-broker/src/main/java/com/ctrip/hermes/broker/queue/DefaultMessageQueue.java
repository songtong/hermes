package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueue extends AbstractMessageQueue {

	private MetaService m_metaService;

	private BrokerConfig m_config;

	public DefaultMessageQueue(String topic, int partition, MessageQueueStorage storage, MetaService metaService,
	      BrokerConfig config) {
		super(topic, partition, storage);
		m_metaService = metaService;
		m_config = config;
	}

	@Override
	protected MessageQueueDumper createDumper(Lease lease) {
		return new DefaultMessageQueueDumper(m_topic, m_partition, m_storage, m_config, lease);
	}

	@Override
	protected MessageQueueCursor create(String groupId, Lease lease) {
		return new DefaultMessageQueueCursor(new Tpg(m_topic, m_partition, groupId), lease, m_storage, m_metaService);
	}

	@Override
	protected void doNack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, Integer>> msgSeqs) {
		m_storage.nack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgSeqs);
	}

	@Override
	protected void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		m_storage.ack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgSeq);
	}

}
