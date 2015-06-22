package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.ctrip.hermes.metaserver.commons.BaseZkWatcher;

public abstract class GuardedWatcher extends BaseZkWatcher {
	protected int m_version;

	protected WatcherGuard m_guard;

	public GuardedWatcher(int version, WatcherGuard guard, ExecutorService executor, EventType... acceptedEventTypes) {
		super(executor, acceptedEventTypes);

		m_version = version;
		m_guard = guard;
	}

	@Override
	protected boolean conditionSatisfy(WatchedEvent event) {
		return m_guard.pass(m_version);
	}

}
