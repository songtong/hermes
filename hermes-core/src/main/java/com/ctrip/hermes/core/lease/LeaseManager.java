package com.ctrip.hermes.core.lease;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface LeaseManager<T> {
	public void registerAcquisition(T key, LeaseAcquisitionListener listener);

	public static interface LeaseAcquisitionListener {
		public void onAcquire(Lease lease);

		public void onExpire();
	}
}
