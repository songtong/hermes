package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.concurrent.atomic.AtomicInteger;

import org.unidal.lookup.annotation.Named;

@Named(type = WatcherGuard.class)
public class DefaultWatcherGuard implements WatcherGuard {

	private AtomicInteger m_curVersion = new AtomicInteger(0);

	public DefaultWatcherGuard() {
	}

	@Override
	public int updateVersion() {
		return m_curVersion.incrementAndGet();
	}

	@Override
	public boolean pass(int version) {
		return m_curVersion.get() == version;
	}

	@Override
	public int getVersion() {
		return m_curVersion.get();
	}

}
