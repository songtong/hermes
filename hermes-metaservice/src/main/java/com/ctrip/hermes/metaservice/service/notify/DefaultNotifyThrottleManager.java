package com.ctrip.hermes.metaservice.service.notify;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

@Named(type = NotifyThrottleManager.class)
public class DefaultNotifyThrottleManager implements NotifyThrottleManager, Initializable {
	private static final int DFT_CHECK_COUNT = 10;

	private static final long MIN_CHECK_INTERVAL_MILLIS = 100;

	private static final Logger log = LoggerFactory.getLogger(DefaultNotifyThrottleManager.class);

	private ConcurrentHashMap<String, NotifyThrottle> m_throttles = new ConcurrentHashMap<>();

	private AtomicLong m_nextScheduleTime = new AtomicLong(Long.MAX_VALUE);

	private AtomicReference<ThrottleCleanTask> m_task = new AtomicReference<>();

	private AtomicReference<ScheduledExecutorService> m_executor = new AtomicReference<>();

	@Override
	public NotifyThrottle createThrottle(String key, long limit, long intervalMillis) {
		return createThrottle(key, limit, intervalMillis, DFT_CHECK_COUNT);
	}

	@Override
	public synchronized NotifyThrottle createThrottle(String key, long limit, long intervalMillis, int segmentCount) {
		if (!m_throttles.containsKey(key)) {
			DefaultNotifyThrottle throttle = new DefaultNotifyThrottle(limit, intervalMillis, segmentCount);
			m_throttles.put(key, throttle);
			updateCleanTaskIfNeeded(throttle.getCheckIntervalMillis());
			return throttle;
		}
		throw new RuntimeException(String.format("Throttle with key %s already exists.", key));
	}

	private void updateCleanTaskIfNeeded(long newInterval) {
		newInterval = newInterval < MIN_CHECK_INTERVAL_MILLIS ? MIN_CHECK_INTERVAL_MILLIS : newInterval;
		if (newInterval < m_task.get().getInterval()) {
			m_task.get().setInterval(newInterval);
			if (m_nextScheduleTime.get() - System.currentTimeMillis() > newInterval) {
				updateExecutor();
			}
		}
	}

	private void updateExecutor() {
		synchronized (m_executor) {
			try {
				if (m_executor.get() != null) {
					m_executor.get().shutdown();
				}
			} catch (Exception e) {
				log.error("Shutdown expire-cleaner executor failed.", e);
			} finally {
				m_executor.set(Executors.newSingleThreadScheduledExecutor());
				m_executor.get().schedule(m_task.get(), m_task.get().getInterval(), TimeUnit.MILLISECONDS);
				m_nextScheduleTime.set(System.currentTimeMillis() + m_task.get().getInterval());
			}
		}
	}

	private class ThrottleCleanTask implements Runnable {
		private volatile long m_interval = Long.MAX_VALUE;

		public ThrottleCleanTask setInterval(long interval) {
			m_interval = interval;
			return this;
		}

		public long getInterval() {
			return m_interval;
		}

		@Override
		public void run() {
			try {
				long interval = m_interval;
				for (Entry<String, NotifyThrottle> entry : m_throttles.entrySet()) {
					((DefaultNotifyThrottle) entry.getValue()).update(interval);
				}
			} catch (Exception e) {
				log.error("Execute throttle update task failed!", e);
			} finally {
				m_executor.get().schedule(this, m_interval, TimeUnit.MILLISECONDS);
				m_nextScheduleTime.set(System.currentTimeMillis() + m_interval);
			}
		}
	}

	@Override
	public synchronized void deregister(String key) {
		DefaultNotifyThrottle removed = (DefaultNotifyThrottle) m_throttles.remove(key);
		if (removed != null && removed.getCheckIntervalMillis() <= m_task.get().getInterval()) {
			long newInterval = Long.MAX_VALUE;
			for (Entry<String, NotifyThrottle> entry : m_throttles.entrySet()) {
				newInterval = Math.min(((DefaultNotifyThrottle) entry.getValue()).getCheckIntervalMillis(), newInterval);
			}
			m_task.get().setInterval(newInterval);
		}
	}

	@Override
	public void initialize() throws InitializationException {
		m_task.set(new ThrottleCleanTask().setInterval(Long.MAX_VALUE));
	}

	@Override
	public NotifyThrottle getThrottle(String key) {
		return m_throttles.get(key);
	}
}
