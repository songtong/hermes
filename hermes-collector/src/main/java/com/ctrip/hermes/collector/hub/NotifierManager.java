package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.state.NotifiedState;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.notify.NotifyService;

@Component
public class NotifierManager { 
	private static Logger LOGGER = LoggerFactory.getLogger(NotifierManager.class);
	private ExecutorService m_executors;
	private ArrayBlockingQueue<NotifiedState> m_queue;
	private AtomicBoolean m_running = new AtomicBoolean(false);
	private NotifyService m_noticeService = PlexusComponentLocator.lookup(NotifyService.class);
	
	@Autowired
	private CollectorConfiguration conf;
	
	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<NotifiedState>(1000);
		m_executors = Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(NotifierManager.class.getSimpleName()));
		m_executors.submit(new NotifierTask());
		m_running.compareAndSet(false, true);
	}
	
	public void offer(NotifiedState state) {
		if (state == null) {
			return;
		}
		if (!m_queue.offer(state)) {
			LOGGER.warn("Failed to put state {} into notifier manager!", state);
		}
	}
	
	public class NotifierTask implements Runnable {

		@Override
		public void run() {
			try {
				List<NotifiedState> states = new ArrayList<NotifiedState>();
				while (!Thread.interrupted()) {
					if (m_queue.drainTo(states) > 0) {
						for (NotifiedState state : states) {
							m_noticeService.notify(state.getNotice());
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error("Notifier Thead exists due to error: " + e.getMessage(), e);
			} finally {
				m_running.compareAndSet(true, false);
			}
		}
	}
}
