package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.model.MonitorEvent;

public class ProduceLatencyTooLargeEvent extends BaseMonitorEvent {

	private String m_topic;
	
	private String m_brokerGroup;

	private String m_date;

	private double m_latency;
	
	private long m_count;
	
	private long m_countAll;
	
	private double m_ratio;

	public ProduceLatencyTooLargeEvent() {
		this(null, null, 0d);
	}

	public ProduceLatencyTooLargeEvent(String topic, String date, double latency) {
		super(MonitorEventType.PRODUCE_LARGE_LATENCY);
		m_topic = topic;
		m_date = date;
		m_latency = latency;
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
	
	public long getCount() {
		return m_count;
	}

	public void setCount(long count) {
		m_count = count;
	}

	public long getCountAll() {
		return m_countAll;
	}

	public void setCountAll(long countAll) {
		m_countAll = countAll;
	}
	
	public String getBrokerGroup() {
		return m_brokerGroup;
	}

	public void setBrokerGroup(String brokerGroup) {
		m_brokerGroup = brokerGroup;
	}

	public double getRatio() {
		return m_ratio;
	}

	public void setRatio(double ratio) {
		m_ratio = ratio;
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
		e.setMessage(String.format("*[%s]Topic %s produce's latency too large(latency=%sms).", m_date, m_topic, m_latency));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_date == null) ? 0 : m_date.hashCode());
		long temp;
		temp = Double.doubleToLongBits(m_latency);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProduceLatencyTooLargeEvent other = (ProduceLatencyTooLargeEvent) obj;
		if (m_date == null) {
			if (other.m_date != null)
				return false;
		} else if (!m_date.equals(other.m_date))
			return false;
		if (Double.doubleToLongBits(m_latency) != Double.doubleToLongBits(other.m_latency))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}

}
