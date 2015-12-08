package com.ctrip.hermes.consumer.engine.ack;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckManager {
	public void register(long correlationId, Tpg tpg, int maxAckHolderSize);

	public void ack(long correlationId, ConsumerMessage<?> msg);

	public void nack(long correlationId, ConsumerMessage<?> msg);

	public void delivered(long correlationId, ConsumerMessage<?> msg);

	public void deregister(long correlationId);
}
