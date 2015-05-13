package com.ctrip.hermes.consumer.engine.lease;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager.ConsumerLeaseKey;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.AbstractLeaseManager;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.SessionIdAware;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LeaseManager.class, value = "consumer")
public class ConsumerLeaseManager extends AbstractLeaseManager<ConsumerLeaseKey> {
	@Inject
	private MetaService m_metaService;

	@Override
	protected LeaseAcquireResponse tryAcquireLease(ConsumerLeaseKey key) {
		return m_metaService.tryAcquireConsumerLease(key.getTpg(), key.getSessionId());
	}

	@Override
	protected LeaseAcquireResponse tryRenewLease(ConsumerLeaseKey key, Lease lease) {
		return m_metaService.tryRenewConsumerLease(key.getTpg(), lease, key.getSessionId());
	}

	public static class ConsumerLeaseKey implements SessionIdAware {
		private Tpg m_tpg;

		private String m_sessionId;

		public ConsumerLeaseKey(Tpg tpg, String sessionId) {
			m_tpg = tpg;
			m_sessionId = sessionId;
		}

		@Override
		public String getSessionId() {
			return m_sessionId;
		}

		public Tpg getTpg() {
			return m_tpg;
		}

	}
}
