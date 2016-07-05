package com.ctrip.hermes.metaservice.service.notify;

public interface NotifyThrottleManager {
	public NotifyThrottle createThrottle(String key, long limit, long intervalMillis);

	public NotifyThrottle createThrottle(String key, long limit, long intervalMillis, int checkCount);

	public NotifyThrottle getThrottle(String key);

	public void deregister(String key);
}
