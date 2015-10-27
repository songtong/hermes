package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ProduceLatencyTooLargeEvent extends BaseMonitorEvent {

	private String m_topic;

	private String m_date;

	private double m_latency;

	public ProduceLatencyTooLargeEvent() {
		super(MonitorEventType.PRODUCE_LARGE_LATENCY);
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public String getDate() {
		return m_date;
	}

	public void setDate(String date) {
		m_date = date;
	}

	public double getLatency() {
		return m_latency;
	}

	public void setLatency(double latency) {
		m_latency = latency;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_date = dbEntity.getKey2();
		m_latency = Double.parseDouble(dbEntity.getKey3());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_date);
		e.setKey3(Double.toString(m_latency));
	}

}
