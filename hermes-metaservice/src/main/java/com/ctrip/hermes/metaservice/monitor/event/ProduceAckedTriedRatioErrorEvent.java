package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ProduceAckedTriedRatioErrorEvent extends BaseMonitorEvent {

	private String m_topic;

	private String m_timespan;

	private int m_tried;

	private int m_acked;

	public ProduceAckedTriedRatioErrorEvent() {
		this(null, null, 0, 0);
	}

	public ProduceAckedTriedRatioErrorEvent(String topic, String timespan, int tried, int acked) {
		super(MonitorEventType.PRODUCE_ACKED_TRIED_RATIO_ERROR);
		m_topic = topic;
		m_timespan = timespan;
		m_tried = tried;
		m_acked = acked;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public String getTimespan() {
		return m_timespan;
	}

	public void setTimespan(String timespan) {
		m_timespan = timespan;
	}

	public int getTried() {
		return m_tried;
	}

	public void setTried(int tried) {
		m_tried = tried;
	}

	public int getAcked() {
		return m_acked;
	}

	public void setAcked(int acked) {
		m_acked = acked;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_timespan = dbEntity.getKey2();
		m_tried = Integer.parseInt(dbEntity.getKey3());
		m_acked = Integer.parseInt(dbEntity.getKey4());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_timespan);
		e.setKey3(Integer.toString(m_tried));
		e.setKey4(Integer.toString(m_acked));
		e.setMessage(String.format("[%s]Topic %s |Acked - Tried|/Tried error(tried=%s, acked=%s).", m_timespan, m_topic,
		      m_tried, m_acked));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + m_acked;
		result = prime * result + ((m_timespan == null) ? 0 : m_timespan.hashCode());
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
		result = prime * result + m_tried;
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
		ProduceAckedTriedRatioErrorEvent other = (ProduceAckedTriedRatioErrorEvent) obj;
		if (m_acked != other.m_acked)
			return false;
		if (m_timespan == null) {
			if (other.m_timespan != null)
				return false;
		} else if (!m_timespan.equals(other.m_timespan))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		if (m_tried != other.m_tried)
			return false;
		return true;
	}

}
