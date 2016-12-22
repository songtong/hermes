package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.NotifyService;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component
public class NotifierManager {
	private static final String DEFAULT_CATEGORY = "default";
	private static Logger LOGGER = LoggerFactory.getLogger(NotifierManager.class);
	private ArrayBlockingQueue<Pair<HermesNotice, String>> m_queue;
	private NotifyService m_noticeService = PlexusComponentLocator.lookup(NotifyService.class);

	@Autowired
	private CollectorConfiguration m_conf;

	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<>(m_conf.getNotifierQueueSize());
		Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(NotifierManager.class
				.getSimpleName())).submit(new NotifierTask());
	}

	public void offer(HermesNotice notice, String category) {
		if (notice == null) {
			return;
		}
		
		if (category == null) {
			category = DEFAULT_CATEGORY;
		}
	
		if (!m_queue.offer(Pair.from(notice, category))) {
			LOGGER.warn("Failed to put state {} into notifier manager!", notice);
		}
	}
	
	public void setCategoryRate(String category, int interval, TimeUnit timeUnit) {
		m_noticeService.setCategoryDefaultRate(category, interval, timeUnit);
	}

	public class NotifierTask implements Runnable {

		@Override
		public void run() {
			List<Pair<HermesNotice, String>> notices = new ArrayList<>();
			Pair<HermesNotice, String> notice = null;
			while (!Thread.interrupted()) {
				try {
					if ((notice = m_queue.take()) != null) {
						m_noticeService.notify(notice.getKey(), notice.getValue());
					}
					
					if (m_queue.drainTo(notices) > 0) {
						for (Pair<HermesNotice, String> n : notices) {
							m_noticeService.notify(n.getKey(), n.getValue());
						}
						notices.clear();
					}
				} catch (Exception e)  {
					LOGGER.error("Notifier Thead encounters error while sending notice: ", e);
				}
			}
		}
	}
}
