package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public abstract class GuardedWatcher implements Watcher {
	protected int m_version;

	protected WatcherGuard m_guard;

	protected ExecutorService m_executor;

	private Set<EventType> m_acceptedEventTypes = new HashSet<>();

	public GuardedWatcher(int version, WatcherGuard guard, ExecutorService executor, EventType... acceptedEventTypes) {
		m_version = version;
		m_guard = guard;
		m_executor = executor;

		if (acceptedEventTypes != null && acceptedEventTypes.length != 0) {
			m_acceptedEventTypes.addAll(m_acceptedEventTypes);
		}
	}

	@Override
	public void process(final WatchedEvent event) {
		if (m_guard.pass(m_version) && eventTypeMatch(event.getType())) {
			m_executor.submit(new Runnable() {

				@Override
				public void run() {
					doProcess(event);
				}

			});
		}
	}

	private boolean eventTypeMatch(EventType type) {
		return m_acceptedEventTypes.isEmpty() || m_acceptedEventTypes.contains(type);
	}

	protected abstract void doProcess(WatchedEvent event);

}
