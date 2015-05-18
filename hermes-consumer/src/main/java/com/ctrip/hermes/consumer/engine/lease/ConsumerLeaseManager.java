package com.ctrip.hermes.consumer.engine.lease;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager.ConsumerLeaseKey;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.SessionIdAware;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LeaseManager.class, value = BuildConstants.CONSUMER)
public class ConsumerLeaseManager implements LeaseManager<ConsumerLeaseKey> {
	@Inject
	private MetaService m_metaService;

	@Override
	public LeaseAcquireResponse tryAcquireLease(ConsumerLeaseKey key) {
		return m_metaService.tryAcquireConsumerLease(key.getTpg(), key.getSessionId());
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(ConsumerLeaseKey key, Lease lease) {
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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_sessionId == null) ? 0 : m_sessionId.hashCode());
			result = prime * result + ((m_tpg == null) ? 0 : m_tpg.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ConsumerLeaseKey other = (ConsumerLeaseKey) obj;
			if (m_sessionId == null) {
				if (other.m_sessionId != null)
					return false;
			} else if (!m_sessionId.equals(other.m_sessionId))
				return false;
			if (m_tpg == null) {
				if (other.m_tpg != null)
					return false;
			} else if (!m_tpg.equals(other.m_tpg))
				return false;
			return true;
		}

	}
}
