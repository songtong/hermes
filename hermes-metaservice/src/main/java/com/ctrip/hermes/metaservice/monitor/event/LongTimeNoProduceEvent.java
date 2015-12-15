package com.ctrip.hermes.metaservice.monitor.event;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;
import com.ctrip.hermes.metaservice.queue.CreationStamp;

public class LongTimeNoProduceEvent extends BaseMonitorEvent {

	private static final SimpleDateFormat m_formatter = new SimpleDateFormat("yyyy-mm-dd hh:MM:ss");

	private String m_topic;

	private int m_partitionId;

	private CreationStamp m_latest;

	public LongTimeNoProduceEvent() {
		this(null, -1, null);
	}

	public LongTimeNoProduceEvent(String topic, int partitionId, CreationStamp latest) {
		super(MonitorEventType.LONG_TIME_NO_PRODUCE);
		m_topic = topic;
		m_partitionId = partitionId;
		m_latest = latest;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_partitionId = Integer.valueOf(dbEntity.getKey2());
		try {
			m_latest = new CreationStamp(Long.valueOf(dbEntity.getKey3()), m_formatter.parse(dbEntity.getKey4()));
		} catch (Exception e) {
			m_latest = new CreationStamp(Long.valueOf(dbEntity.getKey3()), new Date(0));
		}
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(String.valueOf(m_partitionId));
		e.setKey3(String.valueOf(m_latest.getId()));
		e.setKey4(m_formatter.format(m_latest.getDate()));
		e.setMessage(String.format("[%s] Long time no produce for topic: %s[%s], %s", //
		      e.getCreateTime(), m_topic, m_partitionId, m_latest));
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public int getPartitionId() {
		return m_partitionId;
	}

	public void setPartitionId(int partitionId) {
		m_partitionId = partitionId;
	}

	public CreationStamp getLatest() {
		return m_latest;
	}

	public void setLatest(CreationStamp latest) {
		m_latest = latest;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_latest == null) ? 0 : m_latest.hashCode());
		result = prime * result + m_partitionId;
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
		LongTimeNoProduceEvent other = (LongTimeNoProduceEvent) obj;
		if (m_latest == null) {
			if (other.m_latest != null)
				return false;
		} else if (!m_latest.equals(other.m_latest))
			return false;
		if (m_partitionId != other.m_partitionId)
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
		return "LongTimeNoProduceEvent [m_topic=" + m_topic + ", m_partitionId=" + m_partitionId + ", m_latest="
		      + m_latest + "]";
	}
}
