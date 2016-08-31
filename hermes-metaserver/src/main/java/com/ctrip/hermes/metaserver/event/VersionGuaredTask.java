package com.ctrip.hermes.metaserver.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class VersionGuaredTask implements Task {

	private static final Logger log = LoggerFactory.getLogger(VersionGuaredTask.class);

	private long m_version;

	private Guard m_guard;

	public VersionGuaredTask(long version) {
		m_version = version;
		m_guard = PlexusComponentLocator.lookup(Guard.class);
	}

	@Override
	public void run() throws Exception {
		if (m_guard.pass(m_version)) {
			long start = System.currentTimeMillis();
			try {
				doRun();
			} finally {
				log.info("Task {} cost {}ms.", name(), (System.currentTimeMillis() - start));
			}
		} else {
			log.info("Guard check not pass for task {}(version:{})", name(), m_version);
			onGuardNotPass();
		}
	}

	protected void onGuardNotPass() {

	}

	protected abstract void doRun() throws Exception;

}
