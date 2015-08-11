package com.ctrip.hermes.producer.monitor;

import java.util.concurrent.Future;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface SendMessageAcceptanceMonitor {

	public Future<Boolean> monitor(long correlationId);

	public void cancel(long correlationId);

	public void received(long correlationId, boolean success);

}
