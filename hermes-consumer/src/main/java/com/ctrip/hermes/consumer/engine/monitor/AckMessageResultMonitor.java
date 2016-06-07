package com.ctrip.hermes.consumer.engine.monitor;

import java.util.concurrent.Future;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v5.AckMessageResultCommandV5;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckMessageResultMonitor {

	Future<Pair<Boolean, Endpoint>> monitor(long correlationId);

	void received(AckMessageResultCommandV5 cmd);

	void cancel(long correlationId);

}
