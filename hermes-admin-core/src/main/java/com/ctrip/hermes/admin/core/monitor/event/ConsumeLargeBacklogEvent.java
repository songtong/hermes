package com.ctrip.hermes.admin.core.monitor.event;

import java.util.Date;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.model.MonitorEvent;

public class ConsumeLargeBacklogEvent extends BaseMonitorEvent {

	private String m_topic;

	private String m_group;

	private long m_totalBacklog;

	private Map<Integer, Long> m_backlogDetail;

	public ConsumeLargeBacklogEvent() {
		this(null, null, null);
	}

	public ConsumeLargeBacklogEvent(String topic, String group, Map<Integer, Long> backlogs) {
		super(MonitorEventType.CONSUME_LARGE_BACKLOG);
		m_topic = topic;
		m_group = group;
		m_backlogDetail = backlogs;
		m_totalBacklog = 0;
		if (backlogs != null) {
			for (Long backlog : backlogs.values()) {
				m_totalBacklog += backlog;
			}
		}
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_group = dbEntity.getKey2();
		m_totalBacklog = Long.valueOf(dbEntity.getKey3());
		m_backlogDetail = JSON.parseObject(dbEntity.getKey4(), new TypeReference<Map<Integer, Long>>() {
		});
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_group);
		e.setKey3(String.valueOf(m_totalBacklog));
		e.setKey4(JSON.toJSONString(m_backlogDetail));
		e.setMessage(String.format("[%s] Too much backlog for %s : %s, total: %s", //
		      new Date(), m_topic, m_group, m_totalBacklog));
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public String getGroup() {
		return m_group;
	}

	public void setGroup(String group) {
		m_group = group;
	}

	public long getTotalBacklog() {
		return m_totalBacklog;
	}

	public void setTotalBacklog(long totalBacklog) {
		m_totalBacklog = totalBacklog;
	}

	public Map<Integer, Long> getBacklogDetail() {
		return m_backlogDetail;
	}

	public void setBacklogDetail(Map<Integer, Long> backlogDetail) {
		m_backlogDetail = backlogDetail;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_backlogDetail == null) ? 0 : m_backlogDetail.hashCode());
		result = prime * result + ((m_group == null) ? 0 : m_group.hashCode());
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
		result = prime * result + (int) (m_totalBacklog ^ (m_totalBacklog >>> 32));
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
		ConsumeLargeBacklogEvent other = (ConsumeLargeBacklogEvent) obj;
		if (m_backlogDetail == null) {
			if (other.m_backlogDetail != null)
				return false;
		} else if (!m_backlogDetail.equals(other.m_backlogDetail))
			return false;
		if (m_group == null) {
			if (other.m_group != null)
				return false;
		} else if (!m_group.equals(other.m_group))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		if (m_totalBacklog != other.m_totalBacklog)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ConsumeLargeBacklogEvent [m_topic=" + m_topic + ", m_group=" + m_group + ", m_totalBacklog="
		      + m_totalBacklog + ", m_backlogDetail=" + m_backlogDetail + "]";
	}
}
