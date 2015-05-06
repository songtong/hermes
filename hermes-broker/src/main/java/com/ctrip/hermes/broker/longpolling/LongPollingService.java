package com.ctrip.hermes.broker.longpolling;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface LongPollingService {

	void schedulePush(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime);
}
