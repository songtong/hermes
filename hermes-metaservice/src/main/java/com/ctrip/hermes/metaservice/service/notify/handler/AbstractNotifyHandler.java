package com.ctrip.hermes.metaservice.service.notify.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.metaservice.model.NotificationDao;
import com.ctrip.hermes.metaservice.service.notify.HermesNotification;

public abstract class AbstractNotifyHandler implements NotifyHandler {
	private static final Logger log = LoggerFactory.getLogger(AbstractNotifyHandler.class);

	@Inject
	private NotificationDao m_notificationDao;

	protected void persistNotification(HermesNotification notification) throws Exception {
		try {
			m_notificationDao.insert(notification.toDBENtity());
		} catch (DalException e) {
			log.error("Persist hermes notification failed: {}", notification);
		}
	}
}
