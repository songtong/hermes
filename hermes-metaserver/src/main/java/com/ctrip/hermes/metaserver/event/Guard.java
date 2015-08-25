package com.ctrip.hermes.metaserver.event;

import java.util.concurrent.atomic.AtomicLong;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = Guard.class)
public class Guard {
	private AtomicLong m_version = new AtomicLong(0);

	public long upgradeVersion() {
		return m_version.incrementAndGet();
	}

	public long getVersion() {
		return m_version.get();
	}
}
