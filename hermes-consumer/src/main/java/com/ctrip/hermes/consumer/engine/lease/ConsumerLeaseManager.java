package com.ctrip.hermes.consumer.engine.lease;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.AbstractLeaseManager;
import com.ctrip.hermes.core.lease.Lease;
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
	protected Lease tryAcquireLease(Tpg tpg) {
		return m_metaService.tryAcquireConsumerLease(tpg);
	}

	@Override
	protected boolean tryRenewLease(Tpg tpg, Lease lease) {
		return m_metaService.tryRenewLease(tpg, lease);
	}

}
