package com.ctrip.hermes.metaservice.monitor.event.lease;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ConsumerLeaseTimeoutMonitorEvent extends BaseLeaseEvent {

	public ConsumerLeaseTimeoutMonitorEvent() {
		this(null, null, -1);
	}

	public ConsumerLeaseTimeoutMonitorEvent(LeaseOperation leaseOp, String metaserver, int errorCount) {
		super(MonitorEventType.CONSUMER_LEASE_TIMEOUT, leaseOp, metaserver, errorCount);
	}
}
