package com.ctrip.hermes.consumer.engine.monitor;

import java.util.concurrent.Future;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckMessageResultMonitor {

	Future<Boolean> monitor(long correlationId);

	void received(long correlationId, boolean success);

	void cancel(long correlationId);

}
