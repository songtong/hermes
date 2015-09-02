package com.ctrip.hermes.broker.longpolling;

import io.netty.channel.Channel;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface LongPollingService {

	void schedulePush(Tpg tpg, long correlationId, int batchSize, Channel channel, long expireTime, Lease brokerLease,
	      String sessionId);

	void stop();
}
