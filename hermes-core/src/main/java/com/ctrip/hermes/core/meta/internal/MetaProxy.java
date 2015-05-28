package com.ctrip.hermes.core.meta.internal;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

public interface MetaProxy {

	LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId);

	LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId);

	LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId);

	LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId);

}
