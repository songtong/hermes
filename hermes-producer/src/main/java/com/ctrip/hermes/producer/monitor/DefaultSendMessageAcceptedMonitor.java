package com.ctrip.hermes.producer.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.unidal.lookup.annotation.Named;

import com.google.common.util.concurrent.AbstractFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = SendMessageAcceptedMonitor.class)
public class DefaultSendMessageAcceptedMonitor implements SendMessageAcceptedMonitor {

	private Map<Long, CancelableFuture> m_futures = new HashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Override
	public Future<Boolean> monitor(long correlationId) {
		CancelableFuture future = new CancelableFuture(correlationId);
		m_lock.lock();
		try {
			m_futures.put(correlationId, future);
		} finally {
			m_lock.unlock();
		}
		return future;
	}

	@Override
	public void received(long correlationId, boolean success) {
		m_lock.lock();
		try {
			CancelableFuture future = m_futures.remove(correlationId);
			if (future != null) {
				future.set(success);
			}
		} finally {
			m_lock.unlock();
		}

	}

	private class CancelableFuture extends AbstractFuture<Boolean> {
		private long m_correlationId;

		public CancelableFuture(long correlationId) {
			m_correlationId = correlationId;
		}

		@Override
		public boolean set(Boolean value) {
			return super.set(value);
		}

		@Override
		public boolean setException(Throwable throwable) {
			return super.setException(throwable);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			super.cancel(mayInterruptIfRunning);
			m_lock.lock();
			try {
				m_futures.remove(m_correlationId);
			} finally {
				m_lock.unlock();
			}
			return true;
		}
	}
}
