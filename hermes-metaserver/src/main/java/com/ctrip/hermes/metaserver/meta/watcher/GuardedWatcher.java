package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public abstract class GuardedWatcher implements Watcher {
	protected int m_version;

	protected WatcherGuard m_guard;

	protected ExecutorService m_executor;

	public GuardedWatcher(int version, WatcherGuard guard, ExecutorService executor) {
		m_version = version;
		m_guard = guard;
		m_executor = executor;
	}

	@Override
	public void process(final WatchedEvent event) {
		if (m_guard.pass(m_version)) {
			m_executor.submit(new Runnable() {

				@Override
				public void run() {
					doProcess(event);
				}

			});
		}
	}

	protected abstract void doProcess(WatchedEvent event);

}
