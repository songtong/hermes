package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class BrokerErrorEvent extends BaseMonitorEvent {

	public BrokerErrorEvent() {
		this(MonitorEventType.BROKER_ERROR);
	}

	public BrokerErrorEvent(MonitorEventType type) {
		super(MonitorEventType.BROKER_ERROR);
	}

	private String m_brokerIp;

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_brokerIp);
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_brokerIp = dbEntity.getKey1();
	}

}
