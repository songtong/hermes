package com.ctrip.hermes.metaservice.service.notify.storage;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.metaservice.model.Notification;
import com.ctrip.hermes.metaservice.model.NotificationDao;
import com.ctrip.hermes.metaservice.model.NotificationEntity;
import com.ctrip.hermes.metaservice.service.notify.HermesNotification;
import com.ctrip.hermes.metaservice.service.notify.NotificationType;

@Named
public class DefaultNotificationStorage implements NotificationStorage {
	private static final Logger log = LoggerFactory.getLogger(DefaultNotificationStorage.class);

	@Inject
	private NotificationDao m_dao;

	@Inject
	private TransactionManager m_transactionManager;

	private static final int DEFAULT_NOTIFICATION_TIMEOUT_DAYS = 3;

	private static final String DS_NAME = "fxhermesmetadb";

	@Override
	public List<HermesNotification> findSmsNotifications(boolean queryOnly) {
		List<Notification> ns = queryOnly ? findUnnotifiedNotifications() : findAndUpdateUnnotifiedNotifications();
		List<HermesNotification> list = new ArrayList<>();
		if (ns != null && ns.size() > 0) {
			for (Notification notification : ns) {
				List<String> receivers = JSON.parseObject(notification.getReceivers(), new TypeReference<List<String>>() {
				});
				list.add(new HermesNotification(NotificationType.SMS, receivers, notification.getContent()));
			}
		}
		return list;
	}

	private List<Notification> findAndUpdateUnnotifiedNotifications() {
		boolean isSuccess = false;
		try {
			m_transactionManager.startTransaction(DS_NAME);
			List<Notification> unnotifiedNotifications = //
			m_dao.findUnnotifiedNotifications(NotificationType.SMS.name(), DEFAULT_NOTIFICATION_TIMEOUT_DAYS,
			      NotificationEntity.READSET_FULL);
			m_dao.updateNotifiedStatus(unnotifiedNotifications.toArray(new Notification[unnotifiedNotifications.size()]),
			      NotificationEntity.UPDATESET_FULL);
			isSuccess = true;
			return unnotifiedNotifications;
		} catch (Exception e) {
			log.error("Find and Update sms notifications failed.", e);
			return null;
		} finally {
			if (isSuccess) {
				m_transactionManager.commitTransaction();
			} else {
				m_transactionManager.rollbackTransaction();
			}
		}
	}

	private List<Notification> findUnnotifiedNotifications() {
		try {
			return m_dao.findUnnotifiedNotifications(NotificationType.SMS.name(), DEFAULT_NOTIFICATION_TIMEOUT_DAYS,
			      NotificationEntity.READSET_FULL);
		} catch (Exception e) {
			log.error("Find unnotified monitor event failed.", e);
		}
		return null;
	}
}
