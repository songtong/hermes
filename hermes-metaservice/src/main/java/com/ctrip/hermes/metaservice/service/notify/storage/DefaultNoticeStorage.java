package com.ctrip.hermes.metaservice.service.notify.storage;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeType;
import com.ctrip.hermes.metaservice.service.notify.SmsNoticeContent;

@Named(type = NoticeStorage.class)
public class DefaultNoticeStorage implements NoticeStorage {
	private static final Logger log = LoggerFactory.getLogger(DefaultNoticeStorage.class);

	@Inject
	private NotificationDao m_dao;

	@Inject
	private TransactionManager m_transactionManager;

	private static final int DEFAULT_NOTIFICATION_TIMEOUT_DAYS = 3;

	private static final String DS_NAME = "fxhermesmetadb";

	@Override
	public List<HermesNotice> findSmsNotices(boolean queryOnly) {
		List<Notification> ns = queryOnly ? findUnnotifiedNotices() : findAndUpdateUnnotifiedNotices();
		List<HermesNotice> list = new ArrayList<>();
		if (ns != null && ns.size() > 0) {
			for (Notification notification : ns) {
				List<String> receivers = JSON.parseObject(notification.getReceivers(), new TypeReference<List<String>>() {
				});
				list.add(new HermesNotice(receivers, new SmsNoticeContent(notification.getContent())));
			}
		}
		return list;
	}

	private List<Notification> findAndUpdateUnnotifiedNotices() {
		boolean isSuccess = false;
		try {
			m_transactionManager.startTransaction(DS_NAME);
			List<Notification> unnotifiedNotices = //
			m_dao.findUnnotifiedNotifications(HermesNoticeType.SMS.name(), DEFAULT_NOTIFICATION_TIMEOUT_DAYS,
			      NotificationEntity.READSET_FULL);
			m_dao.updateNotifiedStatus(unnotifiedNotices.toArray(new Notification[unnotifiedNotices.size()]),
			      NotificationEntity.UPDATESET_FULL);
			isSuccess = true;
			return unnotifiedNotices;
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

	private List<Notification> findUnnotifiedNotices() {
		try {
			return m_dao.findUnnotifiedNotifications(HermesNoticeType.SMS.name(), DEFAULT_NOTIFICATION_TIMEOUT_DAYS,
			      NotificationEntity.READSET_FULL);
		} catch (Exception e) {
			log.error("Find unnotified monitor event failed.", e);
		}
		return null;
	}

	@Override
	public String addNotice(HermesNotice notice) throws Exception {
		Notification notification = notice.toNotification();
		notification.setCreateTime(new Date());
		notification.setRefKey(UUID.randomUUID().toString());
		m_dao.insert(notification);
		return notification.getRefKey();
	}

	@Override
	public void updateNotifyTime(String refKey, Date date) throws Exception {
		Notification n = new Notification();
		n.setRefKey(refKey);
		n.setNotifyTime(date);
		m_dao.updateNotifiedStatusByRefKey(n, NotificationEntity.UPDATESET_NOTIFY_TIME);
	}

}
