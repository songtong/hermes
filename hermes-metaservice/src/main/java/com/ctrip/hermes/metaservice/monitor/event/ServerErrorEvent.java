package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public abstract class ServerErrorEvent extends BaseMonitorEvent {
	private String m_host;

	private long m_errorCount;

	public ServerErrorEvent(MonitorEventType type, String host, long errorCount) {
		super(type);
		m_host = host;
		m_errorCount = errorCount;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_host = dbEntity.getKey1();
		m_errorCount = Long.valueOf(dbEntity.getKey2());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_host);
		e.setKey2(String.valueOf(m_errorCount));
		e.setMessage(String.format(getMessageFormat(), getCreateTime(), m_host, m_errorCount));
	}

	abstract String getMessageFormat();

	public String getHost() {
		return m_host;
	}

	public void setHost(String host) {
		m_host = host;
	}

	public long getErrorCount() {
		return m_errorCount;
	}

	public void setErrorCount(long errorCount) {
		m_errorCount = errorCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (m_errorCount ^ (m_errorCount >>> 32));
		result = prime * result + ((m_host == null) ? 0 : m_host.hashCode());
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
		ServerErrorEvent other = (ServerErrorEvent) obj;
		if (m_errorCount != other.m_errorCount)
			return false;
		if (m_host == null) {
			if (other.m_host != null)
				return false;
		} else if (!m_host.equals(other.m_host))
			return false;
		return true;
	}
}
