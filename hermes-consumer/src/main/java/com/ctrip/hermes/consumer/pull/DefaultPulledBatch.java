package com.ctrip.hermes.consumer.pull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.ctrip.hermes.consumer.api.OffsetCommitCallback;
import com.ctrip.hermes.consumer.api.PulledBatch;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.google.common.util.concurrent.SettableFuture;

public class DefaultPulledBatch<T> implements PulledBatch<T> {

	private List<ConsumerMessage<T>> m_messages;

	private RetriveSnapshot<T> m_snapshot;

	private ExecutorService m_callbackExecutor;

	public DefaultPulledBatch(List<ConsumerMessage<T>> msgs, RetriveSnapshot<T> snapshot,
	      ExecutorService callbackExecutor) {
		m_messages = msgs;
		m_snapshot = snapshot;
		m_callbackExecutor = callbackExecutor;
	}

	@Override
	public List<ConsumerMessage<T>> getMessages() {
		return m_messages;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void commitAsync(OffsetCommitCallback callback) {
		if (m_snapshot.isDone()) {
			callback.onComplete(Collections.EMPTY_MAP, null);
		} else {
			m_snapshot.commitAsync(callback, m_callbackExecutor);
		}
	}

	@Override
	public synchronized void commitAsync() {
		commitAsync(null);
	}

	@Override
	public synchronized void commitSync() {
		if (!m_snapshot.isDone()) {
			SettableFuture<Boolean> future = m_snapshot.commitAsync(null, m_callbackExecutor);
			try {
				future.get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				// won't happen
				throw new RuntimeException(e);
			}
		}
	}

}
