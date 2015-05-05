package com.ctrip.hermes.core.lease;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Lease {
	private long m_expireTime;

	public Lease(long expireTime) {
		m_expireTime = expireTime;
	}

	public void setExpireTime(long expireTime) {
		m_expireTime = expireTime;
	}

	long getExpireTime() {
		return m_expireTime;
	}

}
