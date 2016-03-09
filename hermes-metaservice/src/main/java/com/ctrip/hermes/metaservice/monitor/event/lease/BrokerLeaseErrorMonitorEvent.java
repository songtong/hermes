package com.ctrip.hermes.metaservice.monitor.event.lease;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class BrokerLeaseErrorMonitorEvent extends BaseLeaseEvent {

	public BrokerLeaseErrorMonitorEvent() {
		this(null, null, -1);
	}

	public BrokerLeaseErrorMonitorEvent(LeaseOperation leaseOp, String metaserver, int errorCount) {
		super(MonitorEventType.BROKER_LEASE_ERROR, leaseOp, metaserver, errorCount);
	}
}
