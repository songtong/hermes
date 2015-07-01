package com.ctrip.hermes.broker.queue;

import java.util.List;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class NoopMessageQueueCursor implements MessageQueueCursor {

	@Override
	public List<TppConsumerMessageBatch> next(int batchSize) {
		return null;
	}

	@Override
	public void init() {
		// do nothing
	}

	@Override
	public Lease getLease() {
		return null;
	}

	@Override
	public boolean hasError() {
		return false;
	}

	@Override
	public boolean isInited() {
		return true;
	}

	@Override
	public void stop() {

	}

}
