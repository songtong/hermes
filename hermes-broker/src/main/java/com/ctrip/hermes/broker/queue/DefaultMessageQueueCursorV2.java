package com.ctrip.hermes.broker.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.broker.queue.storage.FetchResult;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueueCursorV2 extends AbstractMessageQueueCursor {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueCursorV2.class);

	private MessageQueueStorage m_storage;

	private Offset m_initailOffset;

	public DefaultMessageQueueCursorV2(Tpg tpg, Lease lease, MessageQueueStorage storage, MetaService metaService,
	      DefaultMessageQueue messageQueue, Offset initailOffset) {
		super(tpg, lease, metaService, messageQueue);
		m_storage = storage;
		m_initailOffset = initailOffset;
	}

	@Override
	protected Object loadLastPriorityOffset() {
		return m_initailOffset.getPriorityOffset();
	}

	@Override
	protected Object loadLastNonPriorityOffset() {
		return m_initailOffset.getNonPriorityOffset();
	}

	@Override
	protected Object loadLastResendOffset() {
		return m_initailOffset.getResendOffset();
	}

	@Override
	protected FetchResult fetchPriorityMessages(int batchSize) {
		if (!m_stopped.get()) {
			try {
				return m_storage.fetchMessages(m_priorityTpp, m_priorityOffset, batchSize);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Fetch priority message failed. [{}]", m_priorityTpp, e);
				}
			}
		}
		return null;
	}

	@Override
	protected FetchResult fetchNonPriorityMessages(int batchSize) {
		if (!m_stopped.get()) {
			try {
				return m_storage.fetchMessages(m_nonPriorityTpp, m_nonPriorityOffset, batchSize);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Fetch non priority message failed. [{}]", m_nonPriorityTpp, e);
				}
			}
		}
		return null;
	}

	@Override
	protected FetchResult fetchResendMessages(int batchSize) {
		if (!m_stopped.get()) {
			try {
				return m_storage.fetchResendMessages(m_tpg, m_resendOffset, batchSize);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Fetch resend message failed. [{}]", m_tpg, e);
				}
			}
		}
		return null;
	}

	@Override
	protected void doStop() {

	}

}
