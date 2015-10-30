package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ConsumeDelayTooLargeEvent extends BaseMonitorEvent {

	private String m_topic;

	private String m_consumerGroup;

	private String m_date;

	private double m_delay;

	public ConsumeDelayTooLargeEvent() {
		this(null, null, null, 0d);
	}

	public ConsumeDelayTooLargeEvent(String topic, String consumerGroup, String date, double delay) {
		super(MonitorEventType.CONSUME_LARGE_DELAY);
		m_topic = topic;
		m_consumerGroup = consumerGroup;
		m_date = date;
		m_delay = delay;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public String getConsumerGroup() {
		return m_consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		m_consumerGroup = consumerGroup;
	}

	public String getDate() {
		return m_date;
	}

	public void setDate(String date) {
		m_date = date;
	}

	public double getDelay() {
		return m_delay;
	}

	public void setDelay(double delay) {
		m_delay = delay;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_consumerGroup = dbEntity.getKey2();
		m_date = dbEntity.getKey3();
		m_delay = Double.parseDouble(dbEntity.getKey4());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_consumerGroup);
		e.setKey3(m_date);
		e.setKey4(Double.toString(m_delay));
		e.setMessage(String.format("[%s]Consumer group %s of topic %s consume delay(delay=%sms).", m_date,
		      m_consumerGroup, m_topic, m_delay));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_consumerGroup == null) ? 0 : m_consumerGroup.hashCode());
		result = prime * result + ((m_date == null) ? 0 : m_date.hashCode());
		long temp;
		temp = Double.doubleToLongBits(m_delay);
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
		ConsumeDelayTooLargeEvent other = (ConsumeDelayTooLargeEvent) obj;
		if (m_consumerGroup == null) {
			if (other.m_consumerGroup != null)
				return false;
		} else if (!m_consumerGroup.equals(other.m_consumerGroup))
			return false;
		if (m_date == null) {
			if (other.m_date != null)
				return false;
		} else if (!m_date.equals(other.m_date))
			return false;
		if (Double.doubleToLongBits(m_delay) != Double.doubleToLongBits(other.m_delay))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}

}
