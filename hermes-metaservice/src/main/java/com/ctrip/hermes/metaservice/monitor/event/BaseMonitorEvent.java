package com.ctrip.hermes.metaservice.monitor.event;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public abstract class BaseMonitorEvent implements MonitorEvent {
	public static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private long m_id;

	private Date m_createTime;

	private String m_message;

	private MonitorEventType m_type;

	private Date m_notifyTime;

	public BaseMonitorEvent(MonitorEventType type) {
		m_type = type;
	}

	public MonitorEventType getType() {
		return m_type;
	}

	public Date getCreateTime() {
		return m_createTime;
	}
	
	public void setCreateTime(Date date) {
		m_createTime = date;
	}

	public long getId() {
		return m_id;
	}

	public String getMessage() {
		return m_message;
	}

	public Date getNotifyTime() {
		return m_notifyTime;
	}

	public void setNotifyTime(Date notifyTime) {
		m_notifyTime = notifyTime;
	}

	@Override
	public com.ctrip.hermes.metaservice.model.MonitorEvent toDBEntity() {
		com.ctrip.hermes.metaservice.model.MonitorEvent e = new com.ctrip.hermes.metaservice.model.MonitorEvent();
		e.setEventType(getType().getCode());
		e.setCreateTime(new Date());

		toDBEntity0(e);
		return e;
	}

	@Override
	public void parse(com.ctrip.hermes.metaservice.model.MonitorEvent dbEntity) {
		m_createTime = dbEntity.getCreateTime();
		m_id = dbEntity.getId();
		m_message = dbEntity.getMessage();
		m_notifyTime = dbEntity.getNotifyTime();

		parse0(dbEntity);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_createTime == null) ? 0 : m_createTime.hashCode());
		result = prime * result + (int) (m_id ^ (m_id >>> 32));
		result = prime * result + ((m_message == null) ? 0 : m_message.hashCode());
		result = prime * result + ((m_type == null) ? 0 : m_type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BaseMonitorEvent other = (BaseMonitorEvent) obj;
		if (m_createTime == null) {
			if (other.m_createTime != null)
				return false;
		} else if (!m_createTime.equals(other.m_createTime))
			return false;
		if (m_id != other.m_id)
			return false;
		if (m_message == null) {
			if (other.m_message != null)
				return false;
		} else if (!m_message.equals(other.m_message))
			return false;
		if (m_type != other.m_type)
			return false;
		return true;
	}

	protected abstract void parse0(com.ctrip.hermes.metaservice.model.MonitorEvent dbEntity);

	protected abstract void toDBEntity0(com.ctrip.hermes.metaservice.model.MonitorEvent e);

}
