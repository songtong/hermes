package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ProduceSendCmdFailedRatioErrorEvent extends BaseMonitorEvent {

	private String m_topic;

	private String m_timespan;

	private int m_total;

	private int m_failed;

	public ProduceSendCmdFailedRatioErrorEvent() {
		this(null, null, 0, 0);
	}

	public ProduceSendCmdFailedRatioErrorEvent(String topic, String timespan, int total, int failed) {
		super(MonitorEventType.PRODUCE_SEND_CMD_FAILED_RATIO_ERROR);
		m_topic = topic;
		m_timespan = timespan;
		m_total = total;
		m_failed = failed;
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

	public int getTotal() {
		return m_total;
	}

	public void setTotal(int total) {
		m_total = total;
	}

	public int getFailed() {
		return m_failed;
	}

	public void setFailed(int failed) {
		m_failed = failed;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_timespan = dbEntity.getKey2();
		m_total = Integer.parseInt(dbEntity.getKey3());
		m_failed = Integer.parseInt(dbEntity.getKey4());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_timespan);
		e.setKey3(Integer.toString(m_total));
		e.setKey4(Integer.toString(m_failed));
		e.setMessage(String.format("[%s]Topic %s |Send.Cmd.Failed|/Send.Cmd.Total error(total=%s, failed=%s).", //
		      m_timespan, m_topic, m_total, m_failed));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_timespan == null) ? 0 : m_timespan.hashCode());
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
		ProduceSendCmdFailedRatioErrorEvent other = (ProduceSendCmdFailedRatioErrorEvent) obj;
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
		return true;
	}
}
