package com.ctrip.hermes.admin.core.monitor.event;

import java.util.Date;

import com.ctrip.hermes.admin.core.model.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public class TopicLargeDeadLetterEvent extends BaseMonitorEvent {

	private String m_topic;

	private long m_deadCount;

	private Date m_startDate;

	private Date m_endDate;

	public TopicLargeDeadLetterEvent() {
		this(null, -1, null, null);
	}

	public TopicLargeDeadLetterEvent(String topic, long count, Date start, Date end) {
		super(MonitorEventType.TOPIC_LARGE_DEAD_LETTER);
		m_topic = topic;
		m_deadCount = count;
		m_startDate = start;
		m_endDate = end;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_deadCount = Long.valueOf(dbEntity.getKey2());
		m_startDate = new Date(Long.valueOf(dbEntity.getKey3()));
		m_endDate = new Date(Long.valueOf(dbEntity.getKey4()));
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(String.valueOf(m_deadCount));
		e.setKey3(String.valueOf(m_startDate.getTime()));
		e.setKey4(String.valueOf(m_endDate.getTime()));
		e.setMessage(String.format("Too much dead-letter, topic:%s, count:%s, [%s to %s].", //
		      m_topic, m_deadCount, DATE_FORMATTER.format(m_startDate), DATE_FORMATTER.format(m_endDate)));
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public long getDeadCount() {
		return m_deadCount;
	}

	public void setDeadCount(long deadCount) {
		m_deadCount = deadCount;
	}

	public Date getStartDate() {
		return m_startDate;
	}

	public void setStartDate(Date startDate) {
		m_startDate = startDate;
	}

	public Date getEndDate() {
		return m_endDate;
	}

	public void setEndDate(Date endDate) {
		m_endDate = endDate;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (m_deadCount ^ (m_deadCount >>> 32));
		result = prime * result + ((m_endDate == null) ? 0 : m_endDate.hashCode());
		result = prime * result + ((m_startDate == null) ? 0 : m_startDate.hashCode());
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
		TopicLargeDeadLetterEvent other = (TopicLargeDeadLetterEvent) obj;
		if (m_deadCount != other.m_deadCount)
			return false;
		if (m_endDate == null) {
			if (other.m_endDate != null)
				return false;
		} else if (!m_endDate.equals(other.m_endDate))
			return false;
		if (m_startDate == null) {
			if (other.m_startDate != null)
				return false;
		} else if (!m_startDate.equals(other.m_startDate))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TopicLargeDeadLetterEvent [m_topic=" + m_topic + ", m_deadCount=" + m_deadCount + ", m_startDate="
		      + m_startDate + ", m_endDate=" + m_endDate + "]";
	}

}
