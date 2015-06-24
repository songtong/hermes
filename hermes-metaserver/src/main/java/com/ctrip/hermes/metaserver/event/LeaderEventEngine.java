package com.ctrip.hermes.metaserver.event;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LeaderEventEngine implements EventEngine {

	private EventBus m_eventBus = new DefaultEventBus();

	private ExecutorService m_watcherExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create(
	      "watcherExecutor", true));

	@Override
	public void start(ClusterStateHolder clusterStateHolder) throws Exception {
		m_eventBus.pubEvent(new EventEngineContext(clusterStateHolder, m_eventBus, m_watcherExecutor), new Event(
		      EventType.LEADER_INIT, null));
	}

	@Override
	public void stop() {
		m_eventBus.stop();
		m_watcherExecutor.shutdownNow();
	}

}
