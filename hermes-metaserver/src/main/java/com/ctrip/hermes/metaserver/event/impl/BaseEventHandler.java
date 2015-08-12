package com.ctrip.hermes.metaserver.event.impl;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventEngineContext;
import com.ctrip.hermes.metaserver.event.EventHandler;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseEventHandler implements EventHandler {

	@Override
	public void onEvent(EventEngineContext context, Event event) throws Exception {
		EventBus eventBus = context.getEventBus();
		ClusterStateHolder clusterStateHolder = context.getClusterStateHolder();

		if (eventBus.isStopped()) {
			return;
		}

		if (Role.LEADER == role() && !clusterStateHolder.hasLeadership()) {
			return;
		}

		if (Role.FOLLOWER == role() && clusterStateHolder.hasLeadership()) {
			return;
		}

		processEvent(context, event);
	}

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	protected abstract void processEvent(EventEngineContext context, Event event) throws Exception;

	protected abstract Role role();

	protected enum Role {
		LEADER, //
		FOLLOWER, //
		LEADER_FOLLOWER, //
		;
	}

}
