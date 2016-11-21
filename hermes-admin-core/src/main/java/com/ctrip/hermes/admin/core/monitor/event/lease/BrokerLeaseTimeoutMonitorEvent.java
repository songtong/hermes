package com.ctrip.hermes.admin.core.monitor.event.lease;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public class BrokerLeaseTimeoutMonitorEvent extends BaseLeaseEvent {

	public BrokerLeaseTimeoutMonitorEvent() {
		this(null, null, -1);
	}

	public BrokerLeaseTimeoutMonitorEvent(LeaseOperation leaseOp, String metaserver, int errorCount) {
		super(MonitorEventType.BROKER_LEASE_TIMEOUT, leaseOp, metaserver, errorCount);
	}
}
