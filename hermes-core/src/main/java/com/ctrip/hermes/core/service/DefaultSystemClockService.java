package com.ctrip.hermes.core.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 * @see <a
 *      href="https://github.com/zhongl/jtoolkit/blob/master/common/src/main/java/com/github/zhongl/jtoolkit/SystemClock.java">https://github.com/zhongl/jtoolkit/blob/master/common/src/main/java/com/github/zhongl/jtoolkit/SystemClock.java</a>
 */
@Named(type = SystemClockService.class)
public class DefaultSystemClockService implements SystemClockService, Initializable {
	@Inject
	private CoreConfig m_config;

	// force plexus to init runningStatusStatisticsService
	@Inject
	private RunningStatusStatisticsService m_runningStatusStatService;

	private long m_precision = 1;

	private final AtomicLong m_now = new AtomicLong(System.currentTimeMillis());

	@Override
	public long now() {
		return m_now.get();
	}

	@Override
	public void initialize() throws InitializationException {
		scheduleClockUpdating();
	}

	private void scheduleClockUpdating() {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      "SystemClock", true));
		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				m_now.set(System.currentTimeMillis());
			}
		}, m_precision, m_precision, TimeUnit.MILLISECONDS);
	}

}
