package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventHandler.class, value = "BrokerLeaseChangedEventHandler")
public class BrokerLeaseChangedEventHandler extends BaseEventHandler {

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private EndpointMaker m_endpointMaker;

	@Inject
	private MetaHolder m_metaHolder;

	@Override
	public EventType eventType() {
		return EventType.BROKER_LEASE_CHANGED;
	}

	@Override
	protected void processEvent(Event event) throws Exception {
		Object data = event.getData();
		boolean mergeOnce = false;

		if (data != null) {
			mergeOnce = (Boolean) data;
		}

		m_brokerAssignmentHolder.reassign(new ArrayList<Topic>(m_metaHolder.getMeta().getTopics().values()));
		m_metaHolder.update(m_endpointMaker.makeEndpoints(event.getEventBus(), event.getVersion(),
		      event.getStateHolder(), m_brokerAssignmentHolder.getAssignments(), mergeOnce));
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

}
