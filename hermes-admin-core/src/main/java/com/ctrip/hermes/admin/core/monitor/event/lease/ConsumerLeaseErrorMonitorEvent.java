package com.ctrip.hermes.admin.core.monitor.event.lease;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public class ConsumerLeaseErrorMonitorEvent extends BaseLeaseEvent {

	public ConsumerLeaseErrorMonitorEvent() {
		this(null, null, -1);
	}

	public ConsumerLeaseErrorMonitorEvent(LeaseOperation leaseOp, String metaserver, int errorCount) {
		super(MonitorEventType.CONSUMER_LEASE_ERROR, leaseOp, metaserver, errorCount);
	}
}
