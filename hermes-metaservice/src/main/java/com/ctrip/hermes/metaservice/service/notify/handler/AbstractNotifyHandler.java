package com.ctrip.hermes.metaservice.service.notify.handler;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.NotifyThrottle;
import com.ctrip.hermes.metaservice.service.notify.NotifyThrottleManager;
import com.ctrip.hermes.metaservice.service.notify.storage.NoticeStorage;

public abstract class AbstractNotifyHandler implements NotifyHandler {
	private static final Logger log = LoggerFactory.getLogger(AbstractNotifyHandler.class);

	@Inject
	private NoticeStorage m_noticeStorage;

	@Inject
	private NotifyThrottleManager m_throttleManager;

	private ConcurrentHashMap<String, NotifyThrottle> m_throttles = new ConcurrentHashMap<>();

	@Override
	public synchronized NotifyThrottle setThrottle(String receiver, long limit, long intervalMillis) {
		NotifyThrottle throttle = null;
		if (!m_throttles.containsKey(receiver)) {
			throttle = m_throttleManager.createThrottle(receiver, limit, intervalMillis);
			m_throttles.put(receiver, throttle);
		}
		return throttle;
	}

	protected NotifyThrottle getThrottle(String receiver) {
		NotifyThrottle throttle = m_throttles.get(receiver);
		if (throttle == null) {
			Pair<Long, Long> throttleLimit = getThrottleLimit();
			throttle = setThrottle(receiver, throttleLimit.getKey(), throttleLimit.getValue());
		}
		return throttle;
	}

	@Override
	public boolean handle(HermesNotice notice) {
		String noticeRefKey = persistNotice(notice);
		HermesNotice copy = new HermesNotice(cleanBadReceivers(notice.getReceivers()), notice.getContent());

		filterHermesNoticeReceivers(copy);

		if (copy.getReceivers().size() > 0) {
			boolean handleResult = doHandle(!StringUtils.isBlank(noticeRefKey), copy);
			updateNoticeStatus(copy, handleResult, noticeRefKey);
			return handleResult;
		}
		return true;
	}

	private List<String> cleanBadReceivers(List<String> receivers) {
		List<String> newReceivers = new ArrayList<>();
		for (String receiver : receivers) {
			if (!StringUtils.isBlank(receiver)) {
				newReceivers.add(receiver);
			}
		}
		return newReceivers;
	}

	private void filterHermesNoticeReceivers(HermesNotice notice) {
		Iterator<String> iter = notice.getReceivers().iterator();
		while (iter.hasNext()) {
			String receiver = iter.next();
			if (!getThrottle(receiver).hit()) {
				log.warn("Notice freezed for receiver: {}", receiver);
				iter.remove();
			}
		}
	}

	private String persistNotice(HermesNotice notice) {
		try {
			return m_noticeStorage.addNotice(notice);
		} catch (Exception e) {
			log.warn("Persist notice failed: {}", notice, e);
		}
		return "";
	}

	private void updateNoticeStatus(HermesNotice notice, boolean handleResult, String noticeRefKey) {
		if (handleResult) {
			if (!StringUtils.isBlank(noticeRefKey)) {
				try {
					m_noticeStorage.updateNotifyTime(noticeRefKey, new Date());
				} catch (Exception e) {
					log.warn("Update notice notify time failed: {}", noticeRefKey, e);
				}
			}
		}
	}

	protected abstract boolean doHandle(boolean persisted, HermesNotice notice);

	protected abstract Pair<Long, Long> getThrottleLimit();
}
