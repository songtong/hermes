package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.service.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventHandler.class, value = "BaseMetaChangedEventHandler")
public class BaseMetaChangedEventHandler extends BaseEventHandler {
	private static final Logger log = LoggerFactory.getLogger(BaseMetaChangedEventHandler.class);

	@Inject
	private MetaService m_metaService;

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Inject
	private EndpointMaker m_endpointMaker;

	@Inject
	private MetaHolder m_metaHolder;

	@Override
	public EventType eventType() {
		return EventType.BASE_META_CHANGED;
	}

	@Override
	protected void processEvent(Event event) throws Exception {
		Meta baseMeta = m_metaService.refreshMeta();
		log.info("BaseMeta refreshed(id:{}, version:{}).", baseMeta.getId(), baseMeta.getVersion());

		ArrayList<Topic> topics = new ArrayList<Topic>(baseMeta.getTopics().values());
		List<Endpoint> configedBrokers = baseMeta.getEndpoints() == null ? new ArrayList<Endpoint>() : new ArrayList<>(
		      baseMeta.getEndpoints().values());
		m_brokerAssignmentHolder.reassign(configedBrokers, topics);

		m_metaHolder.setBaseMeta(baseMeta);
		m_metaHolder.update(m_endpointMaker.makeEndpoints(event.getEventBus(), event.getVersion(),
		      event.getStateHolder(), m_brokerAssignmentHolder.getAssignments(), false));

		m_metaServerAssignmentHolder.reassign(null, topics);
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

}
