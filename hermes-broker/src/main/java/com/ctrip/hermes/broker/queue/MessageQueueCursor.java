package com.ctrip.hermes.broker.queue;

import java.util.List;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueueCursor {

	List<TppConsumerMessageBatch> next(int batchSize);

	void init();

	Lease getLease();

}
