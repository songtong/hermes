package com.ctrip.hermes.metaservice.service.notify;

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.RateLimiter;

public interface NotifyService {
	public boolean notify(HermesNotice notice);

	public boolean notify(HermesNotice notice, String category);

	public RateLimiter registerRateLimiter(String receiver, int interval, TimeUnit unit);

	public RateLimiter registerRateLimiter(String receiver, String category, int interval, TimeUnit unit);

	public void setCategoryDefaultRate(String category, int interval, TimeUnit unit);
}
