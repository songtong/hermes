package com.ctrip.hermes.metaservice.monitor.event;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class ConsumerDeadLetterEvent extends BaseMonitorEvent {
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private String m_topic;
	private String m_consumerGroup;
	private long m_deadLetterCount;
	private Date m_startTime;
	private Date m_endTime;
	
	public ConsumerDeadLetterEvent() {
		super(MonitorEventType.CONSUMER_DEAD_LETTER);
	}
	
	public ConsumerDeadLetterEvent(String topic, String consumerGroup, long deadLetterCount, Date startTime, Date endTime) {
		super(MonitorEventType.CONSUMER_DEAD_LETTER);
		this.m_topic = topic;
		this.m_consumerGroup = consumerGroup;
		this.m_deadLetterCount = deadLetterCount;
		this.m_startTime = startTime;
		this.m_endTime = endTime;
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

	public long getDeadLetterCount() {
		return m_deadLetterCount;
	}

	public void setDeadLetterCount(long deadLetterCount) {
		m_deadLetterCount = deadLetterCount;
	}

	public Date getStartTime() {
		return m_startTime;
	}

	public void setStartTime(Date startTime) {
		m_startTime = startTime;
	}

	public Date getEndTime() {
		return m_endTime;
	}

	public void setEndTime(Date endTime) {
		m_endTime = endTime;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_consumerGroup = dbEntity.getKey2();
		m_deadLetterCount= Long.parseLong(dbEntity.getKey3());
		if (dbEntity.getKey4() != null) {
			String[] times = dbEntity.getKey4().split("-");
			if (times.length == 2) {
				try {
					m_startTime = DATE_FORMAT.parse(times[0]);
					m_endTime = DATE_FORMAT.parse(times[1]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_consumerGroup);
		e.setKey3(String.valueOf(m_deadLetterCount));
		e.setKey4(DATE_FORMAT.format(m_startTime) + "-" + DATE_FORMAT.format(m_endTime));
		e.setMessage(String.format("*Dead letter: [topic: %s, consumerGroup: %s, count: %s, startTime: %s, endTime: %s]",
				m_topic, m_consumerGroup, m_deadLetterCount, DATE_FORMAT.format(m_startTime), DATE_FORMAT.format(m_endTime)));
	}

}
