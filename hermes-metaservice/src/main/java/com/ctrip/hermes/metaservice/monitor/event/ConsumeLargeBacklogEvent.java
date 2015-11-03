package com.ctrip.hermes.metaservice.monitor.event;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

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
		for (Long backlog : backlogs.values()) {
			m_totalBacklog += backlog;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_group = dbEntity.getKey2();
		m_totalBacklog = Long.valueOf(dbEntity.getKey3());
		m_backlogDetail = (Map<Integer, Long>) JSON.parse(dbEntity.getKey4());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(m_group);
		e.setKey3(String.valueOf(m_totalBacklog));
		e.setKey4(JSON.toJSONString(m_backlogDetail));
		e.setMessage(String.format("[%s] Too much backlog for %s : %s, total: %s", //
		      getCreateTime(), m_topic, m_group, m_totalBacklog));
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

}
