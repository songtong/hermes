package com.ctrip.hermes.broker.queue;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.ack.internal.AckHolder;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueue extends AbstractMessageQueue {
	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueue.class);

	private MetaService m_metaService;

	private BrokerConfig m_config;

	public DefaultMessageQueue(String topic, int partition, MessageQueueStorage storage, MetaService metaService,
	      BrokerConfig config, ScheduledExecutorService ackOpExecutor) {
		super(topic, partition, storage, ackOpExecutor);
		m_metaService = metaService;
		m_config = config;
	}

	@Override
	protected MessageQueueDumper createDumper(Lease lease) {
		return new DefaultMessageQueueDumper(m_topic, m_partition, m_storage, m_config, lease);
	}

	@Override
	protected MessageQueueCursor create(String groupId, Lease lease) {
		return new DefaultMessageQueueCursor(new Tpg(m_topic, m_partition, groupId), lease, m_storage, m_metaService,
		      this);
	}

	@Override
	protected void doNack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas) {
		m_storage.nack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgId2Metas);
	}

	@Override
	protected void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		m_storage.ack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgSeq);
	}

	@Override
	protected void doStop() {
	}

	@Override
	public Offset findLatestOffset(String groupId) {
		int groupIdInt = m_metaService.translateToIntGroupId(m_topic, groupId);
		try {
			long pOffset = (long) m_storage.findLastOffset(new Tpp(m_topic, m_partition, true), groupIdInt);
			pOffset = getMaxOffset(pOffset, m_forwardOnlyAckHolders.get(new Pair<Boolean, String>(true, groupId)));

			long npOffset = (long) m_storage.findLastOffset(new Tpp(m_topic, m_partition, false), groupIdInt);
			npOffset = getMaxOffset(npOffset, m_forwardOnlyAckHolders.get(new Pair<Boolean, String>(false, groupId)));

			@SuppressWarnings("unchecked")
			Pair<Date, Long> rOffset = (Pair<Date, Long>) m_storage.findLastResendOffset(new Tpg(m_topic, m_partition,
			      groupId));
			return new Offset(pOffset, npOffset, rOffset);
		} catch (Exception e) {
			log.error("Find latest offset failed: topic= {}, partition= {}, group= {}.", m_topic, m_partition, groupId, e);
		}
		return null;
	}

	private long getMaxOffset(long offset, AckHolder<MessageMeta> holder) {
		return holder == null ? offset : Math.max(holder.getMaxAckedOffset(), offset);
	}
}
