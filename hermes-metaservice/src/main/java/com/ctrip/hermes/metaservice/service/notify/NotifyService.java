package com.ctrip.hermes.metaservice.service.notify;

public interface NotifyService {
	public boolean notify(HermesNotice notice);

	public void setThrottle(String receiver, NoticeType type, long limit, long intervalMillis);
}
