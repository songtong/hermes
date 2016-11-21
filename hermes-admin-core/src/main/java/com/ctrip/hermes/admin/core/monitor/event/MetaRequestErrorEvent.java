package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.model.MonitorEvent;

public class MetaRequestErrorEvent extends BaseMonitorEvent {
	public static enum MetaRequestErrorType {
		FAIL, TIMEOUT
	}

	private String m_catType;

	private String m_meta;

	private MetaRequestErrorType m_errorType;

	private int m_errorCount;

	public MetaRequestErrorEvent() {
		this(null, null, null, -1);
	}

	public MetaRequestErrorEvent(String catType, String meta, MetaRequestErrorType errorType, int errorCount) {
		super(MonitorEventType.META_REQUEST_ERROR);
		m_catType = catType;
		m_meta = meta;
		m_errorType = errorType;
		m_errorCount = errorCount;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_catType = dbEntity.getKey1();
		m_meta = dbEntity.getKey2();
		m_errorType = MetaRequestErrorType.valueOf(dbEntity.getKey3());
		m_errorCount = Integer.valueOf(dbEntity.getKey4());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_catType);
		e.setKey2(m_meta);
		e.setKey3(m_errorType.name());
		e.setKey4(String.valueOf(m_errorCount));
		e.setMessage(String.format("[%s] Meta request error. CatType: %s, MetaInfo: %s, ErrorType: %s, ErrorCount: %s",
		      DATE_FORMATTER.format(e.getCreateTime()), m_catType, m_meta, m_errorType, m_errorCount));
	}

	public String getCatType() {
		return m_catType;
	}

	public void setCatType(String catType) {
		m_catType = catType;
	}

	public String getMeta() {
		return m_meta;
	}

	public void setMeta(String meta) {
		m_meta = meta;
	}

	public int getErrorCount() {
		return m_errorCount;
	}

	public void setErrorCount(int errorCount) {
		m_errorCount = errorCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_catType == null) ? 0 : m_catType.hashCode());
		result = prime * result + m_errorCount;
		result = prime * result + ((m_meta == null) ? 0 : m_meta.hashCode());
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
		MetaRequestErrorEvent other = (MetaRequestErrorEvent) obj;
		if (m_catType == null) {
			if (other.m_catType != null)
				return false;
		} else if (!m_catType.equals(other.m_catType))
			return false;
		if (m_errorCount != other.m_errorCount)
			return false;
		if (m_meta == null) {
			if (other.m_meta != null)
				return false;
		} else if (!m_meta.equals(other.m_meta))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MetaRequestErrorEvent [m_catType=" + m_catType + ", m_meta=" + m_meta + ", m_errorType=" + m_errorType
		      + ", m_errorCount=" + m_errorCount + "]";
	}
}
