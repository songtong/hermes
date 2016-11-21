package com.ctrip.hermes.admin.core.monitor.event.lease;

import java.util.Date;

import com.ctrip.hermes.admin.core.model.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.monitor.event.BaseMonitorEvent;

abstract class BaseLeaseEvent extends BaseMonitorEvent implements LeaseOperationAware {
	private String m_metaserver;

	private LeaseOperation m_leaseOperation;

	private int m_errorCount;

	public BaseLeaseEvent(MonitorEventType eventType, LeaseOperation leaseOp, String metaserver, int errorCount) {
		super(eventType);
		m_leaseOperation = leaseOp;
		m_metaserver = metaserver;
		m_errorCount = errorCount;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_leaseOperation = LeaseOperation.valueOf(dbEntity.getKey1());
		m_metaserver = dbEntity.getKey2();
		m_errorCount = Integer.valueOf(dbEntity.getKey3());
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(getLeaseOperation().toString());
		e.setKey2(m_metaserver);
		e.setKey3(String.valueOf(m_errorCount));
		e.setMessage(String.format("[%s] Metaserver: %s, LeaseOperation %s, Error count: %s, ", //
		      DATE_FORMATTER.format(new Date()), getMetaserver(), getLeaseOperation(), getErrorCount()));
	}

	@Override
	public LeaseOperation getLeaseOperation() {
		return m_leaseOperation;
	}

	public void setLeaseOperation(LeaseOperation leaseOperation) {
		m_leaseOperation = leaseOperation;
	}

	public String getMetaserver() {
		return m_metaserver;
	}

	public void setMetaserver(String metaserver) {
		m_metaserver = metaserver;
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
		result = prime * result + m_errorCount;
		result = prime * result + ((m_leaseOperation == null) ? 0 : m_leaseOperation.hashCode());
		result = prime * result + ((m_metaserver == null) ? 0 : m_metaserver.hashCode());
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
		BaseLeaseEvent other = (BaseLeaseEvent) obj;
		if (m_errorCount != other.m_errorCount)
			return false;
		if (m_leaseOperation != other.m_leaseOperation)
			return false;
		if (m_metaserver == null) {
			if (other.m_metaserver != null)
				return false;
		} else if (!m_metaserver.equals(other.m_metaserver))
			return false;
		return true;
	}

}
