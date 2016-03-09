package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ConsumerAckErrorEvent extends BaseMonitorEvent {
	private String m_topic;

	private String m_consumer;

	private int m_total;

	private int m_fails;

	public ConsumerAckErrorEvent() {
		this(null, null, -1, -1);
	}

	public ConsumerAckErrorEvent(String topic, String consumer, int total, int fails) {
		super(MonitorEventType.CONSUMER_ACK_CMD_ERROR);
		m_topic = topic;
		m_consumer = consumer;
		m_total = total;
		m_fails = fails;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_consumer = dbEntity.getKey2();
		m_total = Integer.valueOf(dbEntity.getKey3());
		m_fails = Integer.valueOf(dbEntity.getKey4());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_consumer);
		e.setKey3(String.valueOf(m_total));
		e.setKey4(String.valueOf(m_fails));
		e.setMessage(String.format("[%s] Ack cmd Fails/Total is too large, Topic: %s, Consumer: %s, Ratio: %s.", //
		      DATE_FORMATTER.format(e.getCreateTime()), m_topic, m_consumer, m_fails / (float) m_total));
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

	public int getTotal() {
		return m_total;
	}

	public void setTotal(int total) {
		m_total = total;
	}

	public int getFails() {
		return m_fails;
	}

	public void setFails(int fails) {
		m_fails = fails;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_consumer == null) ? 0 : m_consumer.hashCode());
		result = prime * result + m_fails;
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
		result = prime * result + m_total;
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
		ConsumerAckErrorEvent other = (ConsumerAckErrorEvent) obj;
		if (m_consumer == null) {
			if (other.m_consumer != null)
				return false;
		} else if (!m_consumer.equals(other.m_consumer))
			return false;
		if (m_fails != other.m_fails)
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		if (m_total != other.m_total)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ConsumerAckErrorEvent [m_topic=" + m_topic + ", m_consumer=" + m_consumer + ", m_total=" + m_total
		      + ", m_fails=" + m_fails + "]";
	}
}
