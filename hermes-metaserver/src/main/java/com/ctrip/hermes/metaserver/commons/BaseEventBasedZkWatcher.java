package com.ctrip.hermes.metaserver.commons;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.EventBus;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseEventBasedZkWatcher implements Watcher {

	protected EventBus m_eventBus;

	protected ExecutorService m_executor;

	protected ClusterStateHolder m_clusterStateHolder;

	private Set<EventType> m_acceptedEventTypes = new HashSet<>();

	protected BaseEventBasedZkWatcher(EventBus eventBus, ExecutorService executor,
	      ClusterStateHolder clusterStateHolder, EventType... acceptedEventTypes) {
		m_eventBus = eventBus;
		m_executor = executor;
		m_clusterStateHolder = clusterStateHolder;
		if (acceptedEventTypes != null && acceptedEventTypes.length != 0) {
			m_acceptedEventTypes.addAll(Arrays.asList(acceptedEventTypes));
		}
	}

	@Override
	public void process(final WatchedEvent event) {
		if (conditionSatisfy(event) && eventTypeMatch(event) && !m_eventBus.isStopped() && !m_executor.isShutdown()) {
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
		return m_acceptedEventTypes.isEmpty() || m_acceptedEventTypes.contains(event.getType());
	}

	protected abstract void doProcess(WatchedEvent event);
}
