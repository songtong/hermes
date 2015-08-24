package com.ctrip.hermes.metaserver.cluster.listener;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.Guard;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventBusBootstrapListener.class)
public class EventBusBootstrapListener {

	@Inject
	private EventBus m_eventHandlerExecutor;

	@Inject
	private Guard m_guard;

	public void notLeader(ClusterStateHolder stateHolder) {
		long version = m_guard.upgradeVersion();
		m_eventHandlerExecutor.pubEvent(new Event(EventType.FOLLOWER_INIT, version, stateHolder, null));
	}

	public void isLeader(ClusterStateHolder stateHolder) {
		long version = m_guard.upgradeVersion();
		m_eventHandlerExecutor.pubEvent(new Event(EventType.LEADER_INIT, version, stateHolder, null));
	}

}
