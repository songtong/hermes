package com.ctrip.hermes.collector.state;

import java.util.concurrent.TimeUnit;
// TODO: considering the usage of LinkedList
public class HeadState implements Stateful {
	private long m_timestamp;
	private RetentionPolicy m_policy;
	private int m_size;
	private boolean m_enableRetention;
	private State m_head;
	private State m_tail;

	public RetentionPolicy getPolicy() {
		return m_policy;
	}

	public void setPolicy(RetentionPolicy policy) {
		m_policy = policy;
	}

	public int getSize() {
		return m_size;
	}

	public void setSize(int size) {
		m_size = size;
	}

	public boolean isEnableRetention() {
		return m_enableRetention;
	}

	public void setEnableRetention(boolean enableRetention) {
		this.m_enableRetention = enableRetention;
	}

	public void addState(State state) {
		if (state == null) {
			return;
		}

		if (m_enableRetention) {
			state.setExpiredTime(m_policy.getExpiredTime(state.getTimestamp()));
			if (m_head == null) {
				m_head = state;
				m_tail = state;
			} else {
				state.setNextState(m_head);
				m_head.setPrevState(state);
				m_head = state;
				m_head.getNextState().update(state);
			}

			if (m_policy.getRetentionLength() > 0) {
				// If Already up to the retained size, remove the last state.
				if (m_size == m_policy.getRetentionLength()) {
					State removed = m_tail;
					m_tail = removed.getPrevState();
					m_tail.setNextState(null);
					removed.setPrevState(null);
				}
			}
		}

		state.notifyObservers();
	}

	@Override
	public boolean expired() {
		return false;
	}

	@Override
	public long getTimestamp() {
		return m_timestamp;
	}

	@Override
	public void update(Stateful s) {
		// TODO fix updating when using meta.
	}

	public class RetentionPolicy {
		private TimeUnit m_timeUnit;
		private long m_retentionTime;
		private long m_retentionLength;

		public TimeUnit getTimeUnit() {
			return m_timeUnit;
		}

		public void setTimeUnit(TimeUnit timeUnit) {
			m_timeUnit = timeUnit;
		}

		public long getRetentionTime() {
			return m_retentionTime;
		}

		public void setRetentionTime(long retentionTime) {
			m_retentionTime = retentionTime;
		}

		public long getRetentionLength() {
			return m_retentionLength;
		}

		public void setRetentionLength(long retentionLength) {
			m_retentionLength = retentionLength;
		}
		
		public long getExpiredTime(long milliseconds) {
			if (m_retentionTime > 0) {
				return milliseconds + m_timeUnit.toMillis(m_retentionTime);
			}
			return -1;
		}

	}
}
