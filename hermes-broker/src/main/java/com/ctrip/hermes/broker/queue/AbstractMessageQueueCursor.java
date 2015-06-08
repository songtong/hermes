package com.ctrip.hermes.broker.queue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage.FetchResult;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageQueueCursor implements MessageQueueCursor {
	private static final Logger log = LoggerFactory.getLogger(AbstractMessageQueueCursor.class);

	protected static final int STATE_NOT_INITED = 0;

	protected static final int STATE_INITING = 1;

	protected static final int STATE_INITED = 2;

	protected static final int STATE_INIT_ERROR = 0;

	protected Tpg m_tpg;

	protected Tpp m_priorityTpp;

	protected Tpp m_nonPriorityTpp;

	protected Object m_priorityOffset;

	protected Object m_nonPriorityOffset;

	protected Object m_resendOffset;

	protected MetaService m_metaService;

	protected int m_groupIdInt;

	protected Lease m_lease;

	protected AtomicInteger m_state = new AtomicInteger(STATE_NOT_INITED);

	public AbstractMessageQueueCursor(Tpg tpg, Lease lease, MetaService metaService) {
		m_tpg = tpg;
		m_lease = lease;
		m_priorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), true);
		m_nonPriorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), false);
		m_metaService = metaService;
		m_groupIdInt = m_metaService.translateToIntGroupId(m_tpg.getTopic(), m_tpg.getGroupId());
	}

	@Override
	public void init() {
		if (m_state.compareAndSet(STATE_NOT_INITED, STATE_INITING)) {
			try {
				m_priorityOffset = loadLastPriorityOffset();
				m_nonPriorityOffset = loadLastNonPriorityOffset();
				m_resendOffset = loadLastResendOffset();
				m_state.set(STATE_INITED);
			} catch (Exception e) {
				log.error("Failed to init cursor", e);
				m_state.set(STATE_INIT_ERROR);
				throw e;
			}
		}
	}

	public Lease getLease() {
		return m_lease;
	}

	public boolean hasError() {
		return m_state.get() == STATE_INIT_ERROR;
	}

	public boolean isInited() {
		return m_state.get() == STATE_INITED;
	}

	protected abstract Object loadLastPriorityOffset();

	protected abstract Object loadLastNonPriorityOffset();

	protected abstract Object loadLastResendOffset();

	protected abstract FetchResult fetchPriortyMessages(int batchSize);

	protected abstract FetchResult fetchNonPriortyMessages(int batchSize);

	protected abstract FetchResult fetchResendMessages(int batchSize);

	@Override
	public synchronized List<TppConsumerMessageBatch> next(int batchSize) {
		try {
			List<TppConsumerMessageBatch> result = new LinkedList<>();
			int remainingSize = batchSize;
			FetchResult pFetchResult = fetchPriortyMessages(batchSize);

			if (pFetchResult != null) {
				TppConsumerMessageBatch priorityMessageBatch = pFetchResult.getBatch();
				if (priorityMessageBatch != null && priorityMessageBatch.size() > 0) {
					result.add(priorityMessageBatch);
					remainingSize -= priorityMessageBatch.size();
					m_priorityOffset = pFetchResult.getOffset();
				}
			}

			if (remainingSize > 0) {
				FetchResult rFetchResult = fetchResendMessages(remainingSize);

				if (rFetchResult != null) {
					TppConsumerMessageBatch resendMessageBatch = rFetchResult.getBatch();
					if (resendMessageBatch != null && resendMessageBatch.size() > 0) {
						result.add(resendMessageBatch);
						remainingSize -= resendMessageBatch.size();
						m_resendOffset = rFetchResult.getOffset();
					}
				}
			}

			if (remainingSize > 0) {
				FetchResult npFetchResult = fetchNonPriortyMessages(remainingSize);

				if (npFetchResult != null) {
					TppConsumerMessageBatch nonPriorityMessageBatch = npFetchResult.getBatch();
					if (nonPriorityMessageBatch != null && nonPriorityMessageBatch.size() > 0) {
						result.add(nonPriorityMessageBatch);
						remainingSize -= nonPriorityMessageBatch.size();
						m_nonPriorityOffset = npFetchResult.getOffset();
					}
				}
			}

			return result;
		} catch (Exception e) {
			// TODO
		}

		return null;

	}

}
