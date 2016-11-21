package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.model.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public class CheckerExceptionEvent extends BaseMonitorEvent {

	private String m_checkerName;

	private String m_date;

	private String m_detail;

	public CheckerExceptionEvent() {
		this(null, null, null);
	}

	public CheckerExceptionEvent(String checkerName, String date, String detail) {
		super(MonitorEventType.CHECKER_EXCEPTION);
		m_checkerName = checkerName;
		m_date = date;
		m_detail = detail;
	}

	public String getCheckerName() {
		return m_checkerName;
	}

	public void setCheckerName(String checkerName) {
		m_checkerName = checkerName;
	}

	public String getDate() {
		return m_date;
	}

	public void setDate(String date) {
		m_date = date;
	}

	public String getDetail() {
		return m_detail;
	}

	public void setDetail(String detail) {
		m_detail = detail;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_checkerName = dbEntity.getKey1();
		m_date = dbEntity.getKey2();
		m_detail = dbEntity.getMessage();
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_checkerName);
		e.setKey2(m_date);
		e.setMessage(m_detail);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_checkerName == null) ? 0 : m_checkerName.hashCode());
		result = prime * result + ((m_date == null) ? 0 : m_date.hashCode());
		result = prime * result + ((m_detail == null) ? 0 : m_detail.hashCode());
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
		CheckerExceptionEvent other = (CheckerExceptionEvent) obj;
		if (m_checkerName == null) {
			if (other.m_checkerName != null)
				return false;
		} else if (!m_checkerName.equals(other.m_checkerName))
			return false;
		if (m_date == null) {
			if (other.m_date != null)
				return false;
		} else if (!m_date.equals(other.m_date))
			return false;
		if (m_detail == null) {
			if (other.m_detail != null)
				return false;
		} else if (!m_detail.equals(other.m_detail))
			return false;
		return true;
	}

}
