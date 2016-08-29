package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.BaseNodeCacheListener;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus.Task;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventHandler.class, value = "FollowerInitEventHandler")
public class FollowerInitEventHandler extends BaseEventHandler implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(FollowerInitEventHandler.class);

	@Inject
	protected MetaHolder m_metaHolder;

	@Inject
	protected BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	protected MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Inject
	protected ZKClient m_zkClient;

	@Inject
	protected LeaderMetaFetcher m_leaderMetaFetcher;

	@Inject
	protected MetaService m_metaService;

	protected ScheduledExecutorService m_scheduledExecutor;

	protected NodeCache m_baseMetaVersionCache;

	protected NodeCache m_leaderMetaVersionCache;

	protected NodeCache m_metaServerAssignmentVersionCache;

	public void setMetaHolder(MetaHolder metaHolder) {
		m_metaHolder = metaHolder;
	}

	public void setBrokerAssignmentHolder(BrokerAssignmentHolder brokerAssignmentHolder) {
		m_brokerAssignmentHolder = brokerAssignmentHolder;
	}

	public void setMetaServerAssignmentHolder(MetaServerAssignmentHolder metaServerAssignmentHolder) {
		m_metaServerAssignmentHolder = metaServerAssignmentHolder;
	}

	public void setZkClient(ZKClient zkClient) {
		m_zkClient = zkClient;
	}

	public void setLeaderMetaFetcher(LeaderMetaFetcher leaderMetaFetcher) {
		m_leaderMetaFetcher = leaderMetaFetcher;
	}

	@Override
	public EventType eventType() {
		return EventType.FOLLOWER_INIT;
	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory
		      .create("FollowerRetry", true));
		try {
			m_baseMetaVersionCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getBaseMetaVersionZkPath());
			m_baseMetaVersionCache.start(true);

			m_leaderMetaVersionCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getMetaInfoZkPath());
			m_leaderMetaVersionCache.start(true);

			m_metaServerAssignmentVersionCache = new NodeCache(m_zkClient.get(),
			      ZKPathUtils.getMetaServerAssignmentRootZkPath());
			m_metaServerAssignmentVersionCache.start(true);
		} catch (Exception e) {
			throw new InitializationException(String.format("Init {} failed.", getName()), e);
		}

	}

	@Override
	protected void processEvent(Event event) throws Exception {
		long version = event.getVersion();

		m_brokerAssignmentHolder.clear();

		loadAndAddBaseMetaVersionListener(version);
		loadAndAddLeaderMetaVersionListener(version);
		loadAndAddMetaServerAssignmentVersionListener(version);
	}

	protected void loadAndAddBaseMetaVersionListener(long version) throws DalException {
		ListenerContainer<NodeCacheListener> listenerContainer = m_baseMetaVersionCache.getListenable();
		listenerContainer.addListener(new BaseMetaVersionListener(version, listenerContainer), m_eventBus.getExecutor());
		fetchBaseMetaAndHandleChanged(m_clusterStateHolder);
	}

	protected void loadAndAddLeaderMetaVersionListener(long version) {
		ListenerContainer<NodeCacheListener> listenerContainer = m_leaderMetaVersionCache.getListenable();
		listenerContainer
		      .addListener(new LeaderMetaVersionListener(version, listenerContainer), m_eventBus.getExecutor());
		loadLeaderMeta(version);
	}

	protected void loadAndAddMetaServerAssignmentVersionListener(long version) {
		ListenerContainer<NodeCacheListener> listenerContainer = m_metaServerAssignmentVersionCache.getListenable();
		listenerContainer.addListener(new MetaServerAssignmentVersionListener(version, listenerContainer),
		      m_eventBus.getExecutor());
		m_metaServerAssignmentHolder.reload();
	}

	protected void loadLeaderMeta(final long version) {
		MetaInfo metaInfo = ZKSerializeUtils.deserialize(m_leaderMetaVersionCache.getCurrentData().getData(),
		      MetaInfo.class);
		Meta meta = m_leaderMetaFetcher.fetchMetaInfo(metaInfo);

		if (meta != null && (m_metaHolder.getMeta() == null || meta.getVersion() != m_metaHolder.getMeta().getVersion())) {
			m_metaHolder.setMeta(meta);
			log.info("[{}]Fetched meta from leader(endpoint={}:{},version={})", role(), metaInfo.getHost(),
			      metaInfo.getPort(), meta.getVersion());
		} else if (meta == null) {
			delayRetry(version, m_scheduledExecutor, new Task() {

				@Override
				public void run() {
					try {
						loadLeaderMeta(version);
					} catch (Exception e) {
						log.error("[{}]Exception occurred while loading meta from leader.", role(), e);
					}
				}

				@Override
				public void onGuardNotPass() {
					// do nothing

				}
			});
		}
	}

	protected void fetchBaseMetaAndHandleChanged(ClusterStateHolder clusterStateHolder) throws DalException {
		Meta baseMeta = m_metaService.refreshMeta();
		log.info("[{}]BaseMeta refreshed(id:{}, version:{}).", role(), baseMeta.getId(), baseMeta.getVersion());

		List<Server> configedMetaServers = baseMeta.getServers() == null ? new ArrayList<Server>()
		      : new ArrayList<Server>(baseMeta.getServers().values());
		List<Idc> idcs = baseMeta.getIdcs() == null ? new ArrayList<Idc>() : new ArrayList<Idc>(baseMeta.getIdcs()
		      .values());

		m_metaHolder.setConfigedMetaServers(configedMetaServers);
		m_metaHolder.setIdcs(idcs);

		handleBaseMetaChanged(baseMeta, clusterStateHolder);
	}

	protected void handleBaseMetaChanged(Meta baseMeta, ClusterStateHolder clusterStateHolder) {
		Server server = getCurServerAndFixStatusByIDC(baseMeta);

		if (server == null || !server.isEnabled()) {
			log.info("[{}]Marked down!", role());
			clusterStateHolder.becomeObserver();
		}
	}

	@Override
	protected Role role() {
		return Role.FOLLOWER;
	}

	protected class BaseMetaVersionListener extends BaseNodeCacheListener {

		protected BaseMetaVersionListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void processNodeChanged() {
			try {
				fetchBaseMetaAndHandleChanged(m_clusterStateHolder);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while doing BaseMetaVersionListener.processNodeChanged, will retry.",
				      role(), e);

				delayRetry(m_version, m_scheduledExecutor, new Task() {

					@Override
					public void run() {
						processNodeChanged();
					}

					@Override
					public void onGuardNotPass() {
						// do nothing
					}
				});
			}
		}

		@Override
		protected String getName() {
			return "BaseMetaVersionListener";
		}
	}

	protected class LeaderMetaVersionListener extends BaseNodeCacheListener {

		protected LeaderMetaVersionListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void processNodeChanged() {
			try {
				loadLeaderMeta(m_version);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while handling leader meta watcher event.", role(), e);
			}
		}

		@Override
		protected String getName() {
			return "LeaderMetaVersionListener";
		}
	}

	protected class MetaServerAssignmentVersionListener extends BaseNodeCacheListener {

		protected MetaServerAssignmentVersionListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void processNodeChanged() {
			try {
				m_metaServerAssignmentHolder.reload();
			} catch (Exception e) {
				log.error("[{}]Exception occurred while handling leader meta watcher event.", role(), e);
			}
		}

		@Override
		protected String getName() {
			return "MetaServerAssignmentVersionListener";
		}
	}

}
