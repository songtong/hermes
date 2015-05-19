package com.ctrip.hermes.core.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class HermesThreadFactory implements ThreadFactory {
	private final String m_groupName;

	private final AtomicLong m_threadNumber = new AtomicLong(1);

	private final String m_namePrefix;

	private final boolean m_daemon;

	public static ThreadFactory create(String groupName, String namePrefix, boolean daemon) {
		return new HermesThreadFactory(groupName, namePrefix, daemon);
	}

	private HermesThreadFactory(String groupName, String namePrefix, boolean daemon) {
		m_groupName = groupName;
		m_namePrefix = namePrefix;
		m_daemon = daemon;
	}

	public Thread newThread(Runnable r) {
		Thread t = new Thread(new ThreadGroup(m_groupName), r, m_namePrefix + "-" + m_threadNumber.getAndIncrement());
		t.setDaemon(m_daemon);
		if (t.getPriority() != Thread.NORM_PRIORITY)
			t.setPriority(Thread.NORM_PRIORITY);
		return t;
	}
}
