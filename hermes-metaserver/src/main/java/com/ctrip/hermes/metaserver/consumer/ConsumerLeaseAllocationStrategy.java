package com.ctrip.hermes.metaserver.consumer;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerLeaseAllocationStrategy {
	public LeaseAcquireResponse tryAcquireLease(Tpg tpg, String consumerName);

	public LeaseAcquireResponse tryRenewLease(Tpg tpg, String consumerName, long leaseId);
}
