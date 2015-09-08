package com.ctrip.hermes.broker.ack.internal;

import java.util.List;

import org.unidal.tuple.Pair;

public class ForwardOnlyAckHolder<T> implements AckHolder<T> {
	private long m_maxAckedOffset = -1;

	private boolean m_modified = false;

	@Override
	public void delivered(List<Pair<Long, T>> range, long develiveredTime) {
		// do nothing
	}

	@Override
	public void acked(long offset, boolean success) {
		long oldValue = m_maxAckedOffset;
		m_maxAckedOffset = success ? Math.max(offset, m_maxAckedOffset) : m_maxAckedOffset;
		m_modified = m_maxAckedOffset != oldValue;
	}

	@Override
	public BatchResult<T> scan() {
		if (m_modified) {
			m_modified = false;
			return new BatchResult<T>(null, new ContinuousRange(-1, m_maxAckedOffset));
		}
		return null;
	}

	@Override
	public long getMaxAckedOffset() {
		return m_maxAckedOffset;
	}
}
