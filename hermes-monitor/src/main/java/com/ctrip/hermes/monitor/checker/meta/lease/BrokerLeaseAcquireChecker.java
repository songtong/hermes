package com.ctrip.hermes.monitor.checker.meta.lease;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.metaservice.monitor.event.MetaRequestErrorEvent.MetaRequestErrorType;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.lease.BrokerLeaseErrorMonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.lease.BrokerLeaseTimeoutMonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.lease.LeaseOperationAware.LeaseOperation;
import com.ctrip.hermes.monitor.checker.meta.AbstractMetaRequestErrorChecker;

@Component(value = BrokerLeaseAcquireChecker.ID)
public class BrokerLeaseAcquireChecker extends AbstractMetaRequestErrorChecker {
	public static final String ID = "BrokerLeaseAcquireChecker";

	private static final List<String> CHECKLIST = Arrays.asList("/lease/broker/acquire");

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
		return m_config.getLeaseAcquireTimeoutMillisecondLimit();
	}

	@Override
	protected int getTimeoutCountLimit() {
		return m_config.getLeaseAcquireTimeoutCountLimit();
	}

	@Override
	protected int getErrorCountLimit() {
		return m_config.getLeaseAcquireErrorCountLimit();
	}

	@Override
	protected MonitorEvent generateEvent(String name, String meta, MetaRequestErrorType type, int count) {
		switch (type) {
		case TIMEOUT:
			return new BrokerLeaseTimeoutMonitorEvent(LeaseOperation.ACQUIRE, meta, count);
		case FAIL:
			return new BrokerLeaseErrorMonitorEvent(LeaseOperation.ACQUIRE, meta, count);
		default:
			throw new IllegalArgumentException("Wrong MetaRequestErrorType!");
		}
	}
}