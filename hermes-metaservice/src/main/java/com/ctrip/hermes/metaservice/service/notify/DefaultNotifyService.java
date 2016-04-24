package com.ctrip.hermes.metaservice.service.notify;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.notify.handler.NotifyHandler;

public class DefaultNotifyService implements NotifyService {
	@Override
	public void notify(HermesNotification notification) {
		PlexusComponentLocator.lookup(NotifyHandler.class, notification.getType().handler()).handle(notification);
	}
}
