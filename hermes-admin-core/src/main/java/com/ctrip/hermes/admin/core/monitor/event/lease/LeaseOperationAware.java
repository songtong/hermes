package com.ctrip.hermes.admin.core.monitor.event.lease;

public interface LeaseOperationAware {
	public LeaseOperation getLeaseOperation();

	public static enum LeaseOperation {
		ACQUIRE, RENEW;
	}
}
