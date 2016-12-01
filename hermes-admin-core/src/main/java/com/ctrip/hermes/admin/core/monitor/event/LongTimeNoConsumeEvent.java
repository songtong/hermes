package com.ctrip.hermes.admin.core.monitor.event;

import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.core.utils.StringUtils.StringFormatter;
import com.ctrip.hermes.admin.core.model.MonitorEvent;

public class LongTimeNoConsumeEvent extends BaseMonitorEvent {
	private String m_topic;

	private String m_consumer;
	
	private Map<Integer, CreationStamp> m_latestConsumed;

	public LongTimeNoConsumeEvent() {
		this(null, null, null);
	}

	public LongTimeNoConsumeEvent(String topic, String consumer, Map<Integer, CreationStamp> latestConsumed) {
		super(MonitorEventType.LONG_TIME_NO_CONSUME);
		m_topic = topic;
		m_consumer = consumer;
		m_latestConsumed = latestConsumed;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_consumer = dbEntity.getKey2();
		m_latestConsumed = JSON.parseObject(dbEntity.getKey3(), new TypeReference<Map<Integer, CreationStamp>>() {
		});
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_consumer);
		e.setKey3(JSON.toJSONString(m_latestConsumed));
		e.setMessage(String.format("*[%s] Topic: %s, Consumer: %s, Detail: %s",//
		      DATE_FORMATTER.format(e.getCreateTime()), m_topic, m_consumer, generateDetail()));
	}

	private String generateDetail() {
		return StringUtils.join(m_latestConsumed.entrySet(), ", ", new StringFormatter<Entry<Integer, CreationStamp>>() {
			@Override
			public String format(Entry<Integer, CreationStamp> obj) {
				return //
				obj.getKey() + ":[" + obj.getValue().getId() + "," + DATE_FORMATTER.format(obj.getValue().getDate()) + "]";
			}
		});
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public String getConsumer() {
		return m_consumer;
	}

	public void setConsumer(String consumer) {
		m_consumer = consumer;
	}

	public Map<Integer, CreationStamp> getLatestConsumed() {
		return m_latestConsumed;
	}

	public void setLatestConsumed(Map<Integer, CreationStamp> latestConsumed) {
		m_latestConsumed = latestConsumed;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_consumer == null) ? 0 : m_consumer.hashCode());
		result = prime * result + ((m_latestConsumed == null) ? 0 : m_latestConsumed.hashCode());
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
		LongTimeNoConsumeEvent other = (LongTimeNoConsumeEvent) obj;
		if (m_consumer == null) {
			if (other.m_consumer != null)
				return false;
		} else if (!m_consumer.equals(other.m_consumer))
			return false;
		if (m_latestConsumed == null) {
			if (other.m_latestConsumed != null)
				return false;
		} else if (!m_latestConsumed.equals(other.m_latestConsumed))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}
}
