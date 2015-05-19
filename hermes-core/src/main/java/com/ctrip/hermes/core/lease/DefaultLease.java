package com.ctrip.hermes.core.lease;

import java.util.concurrent.atomic.AtomicLong;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultLease implements Lease {
	private long m_id;

	private AtomicLong m_expireTime = new AtomicLong(0);

	public DefaultLease() {
	}

	public DefaultLease(long id, long expireTime) {
		m_expireTime = new AtomicLong(expireTime);
		m_id = id;
	}

	public void setId(long id) {
		m_id = id;
	}

	public long getId() {
		return m_id;
	}

	public void setExpireTime(long expireTime) {
		m_expireTime.set(expireTime);
	}

	public long getExpireTime() {
		return m_expireTime.get();
	}

	public boolean isExpired() {
		return getRemainingTime() > 0;
	}

	@Override
	public long getRemainingTime() {
		SystemClockService systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);
		return m_expireTime.get() - systemClockService.now();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_id ^ (m_id >>> 32));
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
		DefaultLease other = (DefaultLease) obj;
		if (m_id != other.m_id)
			return false;
		return true;
	}

}
