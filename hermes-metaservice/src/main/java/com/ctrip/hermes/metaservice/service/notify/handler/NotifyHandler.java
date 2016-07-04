package com.ctrip.hermes.metaservice.service.notify.handler;

import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.NotifyThrottle;

public interface NotifyHandler {
	public boolean handle(HermesNotice notice);

	public NotifyThrottle setThrottle(String receiver, long limit, long intervalMillis);
}
