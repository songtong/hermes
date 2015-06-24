package com.ctrip.hermes.metaserver.event;

import java.util.concurrent.ExecutorService;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class EventEngineContext {
	private ClusterStateHolder m_clusterStateHolder;

	private EventBus m_eventBus;

	private ExecutorService m_watcherExecutor;

	public EventEngineContext(ClusterStateHolder clusterStateHolder, EventBus eventBus, ExecutorService watcherExecutor) {
		m_clusterStateHolder = clusterStateHolder;
		m_eventBus = eventBus;
		m_watcherExecutor = watcherExecutor;
	}

	public ClusterStateHolder getClusterStateHolder() {
		return m_clusterStateHolder;
	}

	public EventBus getEventBus() {
		return m_eventBus;
	}

	public ExecutorService getWatcherExecutor() {
		return m_watcherExecutor;
	}

}
