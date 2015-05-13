package com.ctrip.hermes.consumer.engine.lease;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.AbstractLeaseManager;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LeaseManager.class, value = "consumer")
public class ConsumerLeaseManager extends AbstractLeaseManager<Tpg> {
	@Inject
	private MetaService m_metaService;

	@Override
	protected LeaseAcquireResponse tryAcquireLease(Tpg tpg, String sessionId) {
		return m_metaService.tryAcquireConsumerLease(tpg, sessionId);
	}

	@Override
	protected LeaseAcquireResponse tryRenewLease(Tpg tpg, Lease lease, String sessionId) {
		return m_metaService.tryRenewConsumerLease(tpg, lease, sessionId);
	}

}
