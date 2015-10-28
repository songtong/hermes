package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class BrokerErrorEvent extends BaseMonitorEvent {

	private String m_brokerIp;

	public BrokerErrorEvent() {
		super(MonitorEventType.BROKER_ERROR);
	}

	public BrokerErrorEvent broker(String ip) {
		m_brokerIp = ip;
		return this;
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_brokerIp);
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_brokerIp = dbEntity.getKey1();
	}

	@Override
	public String toString() {
		return "BrokerErrorEvent [m_brokerIp=" + m_brokerIp + "]";
	}
}
