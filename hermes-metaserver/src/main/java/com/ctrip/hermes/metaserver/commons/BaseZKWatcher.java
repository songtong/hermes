package com.ctrip.hermes.metaserver.commons;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseZKWatcher implements Watcher {

	protected ExecutorService m_executor;

	private Set<EventType> m_acceptedEventTypes = new HashSet<>();

	protected BaseZKWatcher(ExecutorService executor, EventType... acceptedEventTypes) {
		m_executor = executor;
		if (acceptedEventTypes != null && acceptedEventTypes.length != 0) {
			m_acceptedEventTypes.addAll(Arrays.asList(acceptedEventTypes));
		}
	}

	@Override
	public void process(final WatchedEvent event) {
		if (conditionSatisfy(event) && eventTypeMatch(event)) {
			m_executor.submit(new Runnable() {

				@Override
				public void run() {
					doProcess(event);
				}
			});
		}
	}

	protected boolean conditionSatisfy(final WatchedEvent event) {
		return true;
	}

	protected boolean eventTypeMatch(final WatchedEvent event) {
		return m_acceptedEventTypes.isEmpty() || m_acceptedEventTypes.contains(event.getType())
		      || event.getType() == EventType.None;
	}

	protected abstract void doProcess(WatchedEvent event);
}
