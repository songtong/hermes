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
public abstract class BaseZkWatcher implements Watcher {

	private ExecutorService m_executorService;

	private Set<EventType> m_acceptedEventTypes = new HashSet<>();

	protected BaseZkWatcher(ExecutorService executorService, EventType... acceptedEventTypes) {
		m_executorService = executorService;
		if (acceptedEventTypes != null && acceptedEventTypes.length != 0) {
			m_acceptedEventTypes.addAll(Arrays.asList(acceptedEventTypes));
		}
	}

	@Override
	public void process(final WatchedEvent event) {
		if (m_acceptedEventTypes.isEmpty() || m_acceptedEventTypes.contains(event.getType())) {
			m_executorService.submit(new Runnable() {

				@Override
				public void run() {
					doProcess(event);
				}
			});
		}
	}

	protected abstract void doProcess(WatchedEvent event);
}