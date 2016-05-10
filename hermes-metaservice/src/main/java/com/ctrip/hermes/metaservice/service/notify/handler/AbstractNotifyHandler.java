package com.ctrip.hermes.metaservice.service.notify.handler;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.storage.NoticeStorage;

public abstract class AbstractNotifyHandler implements NotifyHandler, Initializable {
	private static final Logger log = LoggerFactory.getLogger(AbstractNotifyHandler.class);

	@Inject
	private NoticeStorage m_noticeStorage;

	private ConcurrentMap<String, Date> m_freezeExpireTimes = new ConcurrentHashMap<>();

	@Override
	public boolean handle(HermesNotice notice) {
		String noticeRefKey = persistNotice(notice);

		HermesNotice copy = new HermesNotice(new ArrayList<String>(notice.getReceivers()), notice.getContent());

		filterHermesNoticeReceivers(copy);

		if (copy.getReceivers().size() > 0) {
			boolean handleResult = doHandle(!StringUtils.isBlank(noticeRefKey), copy);
			updateNoticeStatus(copy, handleResult, noticeRefKey);
			return handleResult;
		}
		return true;
	}

	private void filterHermesNoticeReceivers(HermesNotice notice) {
		List<String> receivers = notice.getReceivers();
		if (!(receivers instanceof ArrayList)) {
			receivers = new ArrayList<String>(receivers);
			notice.setReceivers(receivers);
		}
		Iterator<String> iter = notice.getReceivers().iterator();
		while (iter.hasNext()) {
			String receiver = iter.next();
			if (isFreeze(receiver)) {
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
		return null;
	}

	private void updateNoticeStatus(HermesNotice notice, boolean handleResult, String noticeRefKey) {
		if (handleResult) {
			updateFreeze(notice);
			if (!StringUtils.isBlank(noticeRefKey)) {
				try {
					m_noticeStorage.updateNotifyTime(noticeRefKey, new Date());
				} catch (Exception e) {
					log.warn("Update notice notify time failed: {}", noticeRefKey, e);
				}
			}
		}
	}

	private boolean isFreeze(String receiver) {
		return m_freezeExpireTimes.containsKey(receiver);
	}

	private void updateFreeze(HermesNotice notice) {
		for (String receiver : notice.getReceivers()) {
			m_freezeExpireTimes.putIfAbsent(receiver,
			      new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(getNotifyIntervalMinute())));
		}
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				Date current = new Date();
				Iterator<Entry<String, Date>> iter = m_freezeExpireTimes.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, Date> item = iter.next();
					if (current.after(item.getValue())) {
						iter.remove();
					}
				}
			}
		}, getNotifyIntervalMinute() * 10, getNotifyIntervalMinute() * 10, TimeUnit.SECONDS);
	}

	protected abstract boolean doHandle(boolean persisted, HermesNotice notice);

	protected abstract int getNotifyIntervalMinute();
}
