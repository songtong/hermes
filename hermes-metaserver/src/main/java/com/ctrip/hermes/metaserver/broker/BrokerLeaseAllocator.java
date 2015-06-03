package com.ctrip.hermes.metaserver.broker;

import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerLeaseAllocator {
	public LeaseAcquireResponse tryAcquireLease(String topic, int partition, String brokerName);

	public LeaseAcquireResponse tryRenewLease(String topic, int partition, String brokerName, long leaseId);

}
