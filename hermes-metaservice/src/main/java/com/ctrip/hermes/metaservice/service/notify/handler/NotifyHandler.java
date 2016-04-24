package com.ctrip.hermes.metaservice.service.notify.handler;

import com.ctrip.hermes.metaservice.service.notify.HermesNotification;

public interface NotifyHandler {
	public void handle(HermesNotification notification);
}
