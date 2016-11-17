package com.ctrip.hermes.monitor.checker.meta.lease;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.MetaRequestErrorEvent.MetaRequestErrorType;
import com.ctrip.hermes.admin.core.monitor.event.lease.BrokerLeaseErrorMonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.lease.BrokerLeaseTimeoutMonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.lease.LeaseOperationAware.LeaseOperation;
import com.ctrip.hermes.monitor.checker.meta.AbstractMetaRequestErrorChecker;

//@Component(value = BrokerLeaseRenewChecker.ID)
public class BrokerLeaseRenewChecker extends AbstractMetaRequestErrorChecker {
	public static final String ID = "BrokerLeaseRenewChecker";

	private static final List<String> CHECKLIST = Arrays.asList("/lease/broker/renew");

	@Override
	public String name() {
		return ID;
	}

	@Override
	protected List<String> getUrlChecklist() {
		return CHECKLIST;
	}

	@Override
	protected int getTimeoutMillisecondLimit() {
		return m_config.getLeaseRenewTimeoutMillisecondLimit();
	}

	@Override
	protected int getTimeoutCountLimit() {
		return m_config.getLeaseRenewTimeoutCountLimit();
	}

	@Override
	protected int getErrorCountLimit() {
		return m_config.getLeaseRenewErrorCountLimit();
	}

	@Override
	protected MonitorEvent generateEvent(String name, String meta, MetaRequestErrorType type, int count) {
		switch (type) {
		case TIMEOUT:
			return new BrokerLeaseTimeoutMonitorEvent(LeaseOperation.RENEW, meta, count);
		case FAIL:
			return new BrokerLeaseErrorMonitorEvent(LeaseOperation.RENEW, meta, count);
		default:
			throw new IllegalArgumentException("Wrong MetaRequestErrorType!");
		}
	}

}
