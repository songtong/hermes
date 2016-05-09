package com.ctrip.hermes.metaservice.service.notify.handler;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.storage.NoticeStorage;

public abstract class AbstractNotifyHandler implements NotifyHandler {
	private static final Logger log = LoggerFactory.getLogger(AbstractNotifyHandler.class);

	@Inject
	private NoticeStorage m_noticeStorage;

	@Override
	public boolean handle(HermesNotice notice) {
		String noticeRefKey = persistNotice(notice);
		boolean handleResult = doHandle(!StringUtils.isBlank(noticeRefKey), notice);
		updateNoticeStatus(handleResult, noticeRefKey);
		return handleResult;
	}

	private String persistNotice(HermesNotice notice) {
		try {
			return m_noticeStorage.addNotice(notice);
		} catch (Exception e) {
			log.warn("Persist notice failed: {}", notice, e);
		}
		return null;
	}

	private void updateNoticeStatus(boolean handleResult, String noticeRefKey) {
		if (handleResult && !StringUtils.isBlank(noticeRefKey)) {
			try {
				m_noticeStorage.updateNotifyTime(noticeRefKey, new Date());
			} catch (Exception e) {
				log.warn("Update notice notify time failed: {}", noticeRefKey, e);
			}
		}
	}

	protected abstract boolean doHandle(boolean persisted, HermesNotice notice);
}
