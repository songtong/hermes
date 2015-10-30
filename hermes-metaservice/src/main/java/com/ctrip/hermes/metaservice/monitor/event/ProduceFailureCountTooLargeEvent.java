package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ProduceFailureCountTooLargeEvent extends BaseMonitorEvent {

	private String m_topic;

	private String m_date;

	private int m_failureCount;

	public ProduceFailureCountTooLargeEvent() {
		this(null, null, 0);
	}

	public ProduceFailureCountTooLargeEvent(String topic, String date, int failureCount) {
		super(MonitorEventType.PRODUCE_LARGE_FAILURE_COUNT);
		m_topic = topic;
		m_date = date;
		m_failureCount = failureCount;
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

	public int getFailureCount() {
		return m_failureCount;
	}

	public void setFailureCount(int failureCount) {
		m_failureCount = failureCount;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_date = dbEntity.getKey2();
		m_failureCount = Integer.parseInt(dbEntity.getKey3());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_date);
		e.setKey3(Integer.toString(m_failureCount));
		e.setMessage(String.format("[%s]Topic %s has too many send error(errorCount=%s).", m_date, m_topic,
		      m_failureCount));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_date == null) ? 0 : m_date.hashCode());
		result = prime * result + m_failureCount;
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
		ProduceFailureCountTooLargeEvent other = (ProduceFailureCountTooLargeEvent) obj;
		if (m_date == null) {
			if (other.m_date != null)
				return false;
		} else if (!m_date.equals(other.m_date))
			return false;
		if (m_failureCount != other.m_failureCount)
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}

}
