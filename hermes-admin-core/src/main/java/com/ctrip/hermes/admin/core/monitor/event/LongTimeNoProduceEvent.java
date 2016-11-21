package com.ctrip.hermes.admin.core.monitor.event;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.model.MonitorEvent;

public class LongTimeNoProduceEvent extends BaseMonitorEvent {

	private String m_topic;

	Map<Integer, Pair<Integer, CreationStamp>> m_limitsAndStamps;

	public LongTimeNoProduceEvent() {
		this(null, null);
	}

	public LongTimeNoProduceEvent(String topic, Map<Integer, Pair<Integer, CreationStamp>> limitsAndStamps) {
		super(MonitorEventType.LONG_TIME_NO_PRODUCE);
		m_topic = topic;
		m_limitsAndStamps = limitsAndStamps;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_limitsAndStamps = JSON.parseObject(dbEntity.getKey3(),
		      new TypeReference<Map<Integer, Pair<Integer, CreationStamp>>>() {
		      });
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(JSON.toJSONString(m_limitsAndStamps.keySet()));
		e.setKey3(JSON.toJSONString(m_limitsAndStamps));
		e.setMessage(generateMessage(m_limitsAndStamps));
	}

	private String generateMessage(Map<Integer, Pair<Integer, CreationStamp>> limitsAndStamps) {
		StringBuilder sb = new StringBuilder(String.format("[%s] Long time no produce(%s): ",
		      DATE_FORMATTER.format(new Date()), m_topic));
		for (Entry<Integer, Pair<Integer, CreationStamp>> entry : limitsAndStamps.entrySet()) {
			sb.append(String.format("[Partition: %s, Limit: %sm, Latest: %s(%s)] ", entry.getKey(), entry.getValue()
			      .getKey(), DATE_FORMATTER.format(entry.getValue().getValue().getDate()), entry.getValue().getKey()));
		}
		return sb.toString();
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public Map<Integer, Pair<Integer, CreationStamp>> getLimitsAndStamps() {
		return m_limitsAndStamps;
	}

	public void setLimitsAndStamps(Map<Integer, Pair<Integer, CreationStamp>> limitsAndStamps) {
		m_limitsAndStamps = limitsAndStamps;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_limitsAndStamps == null) ? 0 : m_limitsAndStamps.hashCode());
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
		if (m_limitsAndStamps == null) {
			if (other.m_limitsAndStamps != null)
				return false;
		} else if (!m_limitsAndStamps.equals(other.m_limitsAndStamps))
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
		return "LongTimeNoProduceEvent [m_topic=" + m_topic + ", m_limitsAndStamps=" + m_limitsAndStamps + "]";
	}
}
