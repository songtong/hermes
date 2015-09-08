package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueueCursor {

	List<TppConsumerMessageBatch> next(int batchSize);

	Pair<Offset, List<TppConsumerMessageBatch>> next(Offset offset, int batchSize);

	void init();

	Lease getLease();

	boolean hasError();

	boolean isInited();

	void stop();

}
