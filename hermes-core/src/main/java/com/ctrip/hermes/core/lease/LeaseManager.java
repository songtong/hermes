package com.ctrip.hermes.core.lease;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface LeaseManager<T extends SessionIdAware> {
	public LeaseAcquireResponse tryAcquireLease(T key);

	public LeaseAcquireResponse tryRenewLease(T key, Lease lease);
}
