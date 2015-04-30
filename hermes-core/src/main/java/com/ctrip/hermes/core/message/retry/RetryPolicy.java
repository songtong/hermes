package com.ctrip.hermes.core.message.retry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface RetryPolicy {

	public int getRetryTimes();

	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis);
}
