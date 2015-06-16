package com.ctrip.hermes.metaserver.meta.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public abstract class GuardedWatcher implements Watcher {
	protected int m_version;

	protected WatcherGuard m_guard;

	public GuardedWatcher(int version, WatcherGuard guard) {
		m_version = version;
		m_guard = guard;
	}

	@Override
	public void process(WatchedEvent event) {
		if (m_guard.pass(m_version)) {
			doProcess(event);
		}
	}

	protected abstract void doProcess(WatchedEvent event);

}
