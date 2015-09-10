package com.ctrip.hermes.broker.queue;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage.FetchResult;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueueCursor extends AbstractMessageQueueCursor {
	private MessageQueueStorage m_storage;

	public DefaultMessageQueueCursor(Tpg tpg, Lease lease, MessageQueueStorage storage, MetaService metaService,
	      MessageQueue messageQueue) {
		super(tpg, lease, metaService, messageQueue);
		m_storage = storage;
	}

	@Override
	protected Object loadLastPriorityOffset() {
		try {
			return m_storage.findLastOffset(m_priorityTpp, m_groupIdInt);
		} catch (Exception e) {
			throw new RuntimeException(String.format(
			      "Load priority message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected Object loadLastNonPriorityOffset() {
		try {
			return m_storage.findLastOffset(m_nonPriorityTpp, m_groupIdInt);
		} catch (Exception e) {
			throw new RuntimeException(String.format(
			      "Load non-priority message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected Object loadLastResendOffset() {
		try {
			return m_storage.findLastResendOffset(m_tpg);
		} catch (Exception e) {
			throw new RuntimeException(String.format(
			      "Load resend message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected FetchResult fetchPriorityMessages(int batchSize) {
		if (!m_stopped.get()) {
			return m_storage.fetchMessages(m_priorityTpp, m_priorityOffset, batchSize);
		} else {
			return null;
		}
	}

	@Override
	protected FetchResult fetchNonPriorityMessages(int batchSize) {
		if (!m_stopped.get()) {
			return m_storage.fetchMessages(m_nonPriorityTpp, m_nonPriorityOffset, batchSize);
		} else {
			return null;
		}
	}

	@Override
	protected FetchResult fetchResendMessages(int batchSize) {
		if (!m_stopped.get()) {
			return m_storage.fetchResendMessages(m_tpg, m_resendOffset, batchSize);
		} else {
			return null;
		}
	}

	@Override
	protected void doStop() {

	}

}
