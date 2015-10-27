package com.ctrip.hermes.metaservice.monitor.event;

import java.util.Date;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public abstract class BaseMonitorEvent implements MonitorEvent {
	private long m_id;

	private Date m_createTime;

	private String m_message;

	private MonitorEventType m_type;

	public BaseMonitorEvent(MonitorEventType type) {
		m_type = type;
	}

	public MonitorEventType getType() {
		return m_type;
	}

	public Date getCreateTime() {
		return m_createTime;
	}

	public long getId() {
		return m_id;
	}

	public String getMessage() {
		return m_message;
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

		parse0(dbEntity);
	}

	protected abstract void parse0(com.ctrip.hermes.metaservice.model.MonitorEvent dbEntity);

	protected abstract void toDBEntity0(com.ctrip.hermes.metaservice.model.MonitorEvent e);

}
