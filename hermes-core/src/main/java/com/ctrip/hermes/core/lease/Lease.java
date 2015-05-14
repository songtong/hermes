package com.ctrip.hermes.core.lease;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Lease {
	private long m_id;

	private AtomicLong m_expireTime = new AtomicLong();

	public Lease() {
	}

	public Lease(long id, long expireTime) {
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

}
