package com.ctrip.hermes.metaservice.service.notify.handler;

import com.ctrip.hermes.metaservice.service.notify.HermesNotice;

public interface NotifyHandler {
	public boolean handle(HermesNotice notice);
}
