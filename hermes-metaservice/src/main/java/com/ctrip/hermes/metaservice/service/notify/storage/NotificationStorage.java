package com.ctrip.hermes.metaservice.service.notify.storage;

import java.util.List;

import com.ctrip.hermes.metaservice.service.notify.HermesNotification;

public interface NotificationStorage {
	public List<HermesNotification> findSmsNotifications(boolean queryOnly);
}
