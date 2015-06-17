package com.ctrip.hermes.metaserver.commons;

import java.util.ArrayList;
import java.util.List;
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

	private List<EventType> m_acceptedEventTypes = new ArrayList<>();

	protected BaseZkWatcher(ExecutorService executorService, EventType... acceptedEventTypes) {
		m_executorService = executorService;
		if (acceptedEventTypes != null && acceptedEventTypes.length != 0) {
			m_acceptedEventTypes.addAll(m_acceptedEventTypes);
		}
	}

	@Override
	public void process(final WatchedEvent event) {
		if (m_acceptedEventTypes.contains(event.getType())) {
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
