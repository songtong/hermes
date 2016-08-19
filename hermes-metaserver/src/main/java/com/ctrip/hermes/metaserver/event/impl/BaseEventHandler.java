package com.ctrip.hermes.metaserver.event.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.Guard;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseEventHandler implements EventHandler {

	private static final Logger log = LoggerFactory.getLogger(BaseEventHandler.class);

	@Inject
	protected MetaServerConfig m_config;

	@Override
	public void onEvent(Event event) throws Exception {
		ClusterStateHolder clusterStateHolder = event.getStateHolder();

		if (clusterStateHolder.getRole() != role()) {
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

	protected Server getCurServerAndFixStatusByIDC(Meta baseMeta) {
		if (baseMeta.getServers() != null && !baseMeta.getServers().isEmpty()) {
			for (Server server : baseMeta.getServers().values()) {
				if (m_config.getMetaServerHost().equals(server.getHost())
				      && m_config.getMetaServerPort() == server.getPort()) {
					Server tmpServer = new Server();
					tmpServer.setHost(server.getHost());
					tmpServer.setId(server.getId());
					tmpServer.setPort(server.getPort());
					tmpServer.setIdc(server.getIdc());
					Idc idc = baseMeta.findIdc(server.getIdc());
					if (idc != null) {
						tmpServer.setEnabled(idc.isEnabled() ? server.isEnabled() : false);
						log.info("MetaServer's enabled is {}(idc:{}, server:{}).", tmpServer.isEnabled(), idc.isEnabled(),
						      server.isEnabled());
					} else {
						tmpServer.setEnabled(false);
						log.info("MetaServer's idc not found(idc={}), will be mark down.", server.getIdc());
					}
					return tmpServer;
				}
			}
		}

		log.info("MetaServer not found in baseMeta, will be marked down.");

		return null;
	}

	protected void delayRetry(final Event event, ScheduledExecutorService executor, final Task task) {
		executor.schedule(new Runnable() {

			@Override
			public void run() {
				if (event.getVersion() == PlexusComponentLocator.lookup(Guard.class).getVersion()) {
					event.getEventBus().getExecutor().submit(new Runnable() {

						@Override
						public void run() {
							task.run();
						}
					});
				}
			}
		}, 2, TimeUnit.SECONDS);
	}

	protected interface Task {
		public void run();
	}
}
