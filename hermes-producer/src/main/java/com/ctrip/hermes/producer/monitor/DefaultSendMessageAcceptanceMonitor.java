package com.ctrip.hermes.producer.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = SendMessageAcceptanceMonitor.class)
public class DefaultSendMessageAcceptanceMonitor implements SendMessageAcceptanceMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultSendMessageAcceptanceMonitor.class);

	private Map<Long, SettableFuture<Boolean>> m_futures = new ConcurrentHashMap<Long, SettableFuture<Boolean>>();

	@Override
	public Future<Boolean> monitor(long correlationId) {
		SettableFuture<Boolean> future = SettableFuture.create();
		m_futures.put(correlationId, future);
		return future;
	}

	@Override
	public void received(long correlationId, boolean success) {
		if (log.isDebugEnabled()) {
			log.debug("Broker acceptance result is {} for correlationId {}", success, correlationId);
		}

		SettableFuture<Boolean> future = m_futures.remove(correlationId);
		if (future != null) {
			future.set(success);
		}
	}

	@Override
	public void cancel(long correlationId) {
		m_futures.remove(correlationId);
	}
}
