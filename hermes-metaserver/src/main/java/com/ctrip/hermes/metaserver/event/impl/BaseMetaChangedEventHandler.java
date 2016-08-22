package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
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

	private ScheduledExecutorService m_scheduledExecutor = Executors
	      .newSingleThreadScheduledExecutor(HermesThreadFactory.create("BaseMetaChangedEventHandlerRetry", true));

	@Override
	public EventType eventType() {
		return EventType.BASE_META_CHANGED;
	}

	@Override
	protected void processEvent(Event event) throws Exception {
		doProcess(event);
	}

	private void doProcess(final Event event) {
		try {
			Meta baseMeta = m_metaService.refreshMeta();
			log.info("Leader BaseMeta refreshed(id:{}, version:{}).", baseMeta.getId(), baseMeta.getVersion());

			Server server = getCurServerAndFixStatusByIDC(baseMeta);

			if (server != null && !server.isEnabled()) {
				log.info("Marked down!");
				event.getStateHolder().becomeObserver();
				return;
			}

			List<Server> configedMetaServers = baseMeta.getServers() == null ? new ArrayList<Server>()
			      : new ArrayList<Server>(baseMeta.getServers().values());
			List<Idc> idcs = baseMeta.getIdcs() == null ? new ArrayList<Idc>() : new ArrayList<Idc>(baseMeta.getIdcs()
			      .values());

			ArrayList<Topic> topics = new ArrayList<Topic>(baseMeta.getTopics().values());
			List<Endpoint> configedBrokers = baseMeta.getEndpoints() == null ? new ArrayList<Endpoint>()
			      : new ArrayList<>(baseMeta.getEndpoints().values());
			m_brokerAssignmentHolder.reassign(configedBrokers, topics);

			m_metaHolder.setIdcs(idcs);
			m_metaHolder.setConfigedMetaServers(configedMetaServers);
			m_metaHolder.setBaseMeta(baseMeta);
			m_metaHolder.update(m_endpointMaker.makeEndpoints(event.getEventBus(), event.getVersion(),
			      event.getStateHolder(), m_brokerAssignmentHolder.getAssignments(), false));

			m_metaServerAssignmentHolder.reassign(null, topics);
		} catch (Exception e) {
			log.error("Exception occurred while processing BaseMetaChanged event, will retry.", e);
			delayRetry(event, m_scheduledExecutor, new Task() {

				@Override
				public void run() {
					doProcess(event);
				}
			});
		}
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

}
