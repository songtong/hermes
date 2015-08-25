package com.ctrip.hermes.metaserver.commons;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.Guard;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseEventBasedZkWatcher implements Watcher {

	protected EventBus m_eventBus;

	protected long m_version;

	private Set<EventType> m_acceptedEventTypes = new HashSet<>();

	protected Guard m_guard;

	protected BaseEventBasedZkWatcher(EventBus eventBus, long version, EventType... acceptedEventTypes) {
		m_eventBus = eventBus;
		m_version = version;
		m_guard = PlexusComponentLocator.lookup(Guard.class);
		if (acceptedEventTypes != null && acceptedEventTypes.length != 0) {
			m_acceptedEventTypes.addAll(Arrays.asList(acceptedEventTypes));
		}
	}

	@Override
	public void process(final WatchedEvent event) {
		if (conditionSatisfy(event) && eventTypeMatch(event)) {
			m_eventBus.getExecutor().submit(new Runnable() {

				@Override
				public void run() {
					doProcess(event);
				}
			});
		}
	}

	protected boolean conditionSatisfy(final WatchedEvent event) {
		return m_version == m_guard.getVersion();
	}

	protected boolean eventTypeMatch(final WatchedEvent event) {
		return m_acceptedEventTypes.isEmpty() || m_acceptedEventTypes.contains(event.getType())
		      || event.getType() == EventType.None;
	}

	protected abstract void doProcess(WatchedEvent event);
}
