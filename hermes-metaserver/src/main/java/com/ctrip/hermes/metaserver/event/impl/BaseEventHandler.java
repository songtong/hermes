package com.ctrip.hermes.metaserver.event.impl;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseEventHandler implements EventHandler {

	@Override
	public void onEvent(Event event) throws Exception {
		ClusterStateHolder clusterStateHolder = event.getStateHolder();

		if (Role.LEADER == role() && !clusterStateHolder.hasLeadership()) {
			return;
		}

		if (Role.FOLLOWER == role() && clusterStateHolder.hasLeadership()) {
			return;
		}

		processEvent(event);
	}

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	protected abstract void processEvent(Event event) throws Exception;

	protected abstract Role role();

	protected enum Role {
		LEADER, //
		FOLLOWER, //
		LEADER_FOLLOWER, //
		;
	}

}
