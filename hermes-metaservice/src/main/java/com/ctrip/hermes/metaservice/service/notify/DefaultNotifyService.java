package com.ctrip.hermes.metaservice.service.notify;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.notify.handler.NotifyHandler;
import com.ctrip.hermes.metaservice.service.notify.storage.NoticeStorage;
import com.google.common.util.concurrent.RateLimiter;

@Named(type = NotifyService.class)
public class DefaultNotifyService implements NotifyService {
	private static final Logger log = LoggerFactory.getLogger(DefaultNotifyService.class);

	private static final String DEFAULT_RATE_LIMITER_CATEGORY = "_DEFAULT_CATEORY";

	@Inject
	private NoticeStorage m_noticeStorage;

	private ConcurrentHashMap<String, RateLimiter> m_rateLimiters = new ConcurrentHashMap<>();

	private ConcurrentHashMap<String, Pair<Integer, TimeUnit>> m_categoryDefaultRate = new ConcurrentHashMap<>();

	@Override
	public boolean notify(HermesNotice notice) {
		return notify(notice, DEFAULT_RATE_LIMITER_CATEGORY);
	}

	@Override
	public boolean notify(HermesNotice notice, String category) {
		if (notice != null && notice.getType() != null) {
			NotifyHandler handler = PlexusComponentLocator.lookup(NotifyHandler.class, notice.getType().handlerID());
			try {
				if (handler != null) {
					String noticeRefKey = persistNotice(notice);
					cleanReceivers(notice, category);
					if (notice.getReceivers().size() > 0) {
						boolean handleResult = handler.handle(notice);
						updateNoticeStatus(notice, handleResult, noticeRefKey);
						return handleResult;
					}
				}
			} catch (Exception e) {
				log.error("Handle hermes notification failed! {}", notice, e);
			}
		}
		return true;
	}

	private void cleanReceivers(HermesNotice notice, String category) {
		Iterator<String> iter = notice.getReceivers().iterator();
		while (iter.hasNext()) {
			String receiver = iter.next();
			if (!isReceiverValid(receiver) || !tryAcquireRateToken(receiver, category, notice.getType())) {
				iter.remove();
			}
		}
	}

	private boolean isReceiverValid(String receiver) {
		return !StringUtils.isBlank(receiver);
	}

	private boolean tryAcquireRateToken(String receiver, String category, HermesNoticeType type) {
		String key = getRateLimiterKey(receiver, category);
		RateLimiter rateLimiter = m_rateLimiters.get(key);
		if (rateLimiter == null) {
			Pair<Integer, TimeUnit> pair = m_categoryDefaultRate.get(category);
			int interval = pair == null ? type.defaultRateInMinute() : pair.getKey();
			TimeUnit unit = pair == null ? TimeUnit.MINUTES : pair.getValue();
			rateLimiter = registerRateLimiter(receiver, category, interval, unit);
		}
		boolean acquired = rateLimiter.tryAcquire();
		if (!acquired) {
			log.warn("Notify operation was freezed for receiver:{}, category:{}", receiver, category);
		}
		return acquired;
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

	@Override
	public RateLimiter registerRateLimiter(String receiver, int interval, TimeUnit unit) {
		return registerRateLimiter(receiver, DEFAULT_RATE_LIMITER_CATEGORY, interval, unit);
	}

	@Override
	public RateLimiter registerRateLimiter(String receiver, String category, int interval, TimeUnit unit) {
		String key = getRateLimiterKey(receiver, category);
		RateLimiter rateLimiter = m_rateLimiters.get(key);
		if (rateLimiter == null) {
			synchronized (m_rateLimiters) {
				if (!m_rateLimiters.containsKey(key)) {
					rateLimiter = RateLimiter.create(1 / (double) unit.toSeconds(interval));
					m_rateLimiters.put(key, rateLimiter);
				}
			}
		}
		return rateLimiter;
	}

	private String getRateLimiterKey(String receiver, String category) {
		return String.format("%s#%s", receiver, category == null ? DEFAULT_RATE_LIMITER_CATEGORY : category);
	}

	@Override
	public void setCategoryDefaultRate(String category, int interval, TimeUnit unit) {
		m_categoryDefaultRate.put(category, new Pair<Integer, TimeUnit>(interval, unit));
	}
}
