package com.ctrip.hermes.core.meta.internal;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.meta.MetaProxy;

@Named(type = MetaProxy.class, value = LocalMetaProxy.ID)
public class LocalMetaProxy implements MetaProxy {

	public final static String ID = "local";

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		// TODO
		long expireTime = System.currentTimeMillis() + 10 * 1000L;
		long leaseId = 123L;
		return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		// TODO
		return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + 10 * 1000L);
	}

}
