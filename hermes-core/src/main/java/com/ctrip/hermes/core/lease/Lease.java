package com.ctrip.hermes.core.lease;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Lease {
	private AtomicLong m_expireTime = new AtomicLong();

	public Lease(long expireTime) {
		m_expireTime = new AtomicLong(expireTime);
	}

	public void setExpireTime(long expireTime) {
		m_expireTime.set(expireTime);
	}

	public long getExpireTime() {
		return m_expireTime.get();
	}

}
