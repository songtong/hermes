package com.ctrip.hermes.metaserver.event.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger log = LoggerFactory.getLogger(BaseEventHandler.class);

	@Override
	public void onEvent(EventEngineContext context, Event event) throws Exception {
		EventBus eventBus = context.getEventBus();
		ClusterStateHolder clusterStateHolder = context.getClusterStateHolder();

		log.info("[FOR_TEST] event handler triggered {} {}.", this.eventType(), this.getName());
		if (eventBus.isStopped()) {
			return;
		}

		if (Role.LEADER == role() && !clusterStateHolder.hasLeadership()) {
			return;
		}

		if (Role.FOLLOWER == role() && clusterStateHolder.hasLeadership()) {
			return;
		}

		log.info("[FOR_TEST] process event {} {}.", this.eventType(), this.getName());
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
