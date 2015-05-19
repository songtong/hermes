package com.ctrip.hermes.core.lease;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface Lease {

	public long getId();

	public void setExpireTime(long expireTime);

	public boolean isExpired();

	public long getRemainingTime();

	public long getExpireTime();

}
