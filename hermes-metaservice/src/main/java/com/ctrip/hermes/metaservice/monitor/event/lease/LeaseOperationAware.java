package com.ctrip.hermes.metaservice.monitor.event.lease;

public interface LeaseOperationAware {
	public LeaseOperation getLeaseOperation();

	public static enum LeaseOperation {
		ACQUIRE, RENEW;
	}
}
