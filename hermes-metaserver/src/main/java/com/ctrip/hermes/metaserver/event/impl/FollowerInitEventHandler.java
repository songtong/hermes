package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
import com.ctrip.hermes.metaserver.commons.BaseEventBasedZkWatcher;
import com.ctrip.hermes.metaserver.event.Event;
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
	}

	@Override
	protected void processEvent(Event event) throws Exception {
		m_brokerAssignmentHolder.clear();
		loadAndAddBaseMetaWatcher(new BaseMetaWatcher(event), event.getStateHolder(), true);

		loadAndAddLeaderMetaWatcher(new LeaderMetaChangedWatcher(event), event);

		loadAndAddMetaServerAssignmentWatcher(new MetaServerAssignmentChangedWatcher(event));
	}

	protected Long loadAndAddBaseMetaWatcher(Watcher watcher, ClusterStateHolder clusterStateHolder, boolean firstTime)
	      throws Exception {
		byte[] data = m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getBaseMetaVersionZkPath());
		if (firstTime) {
			fetchBaseMetaAndHandleChanged(clusterStateHolder);
		}

		return ZKSerializeUtils.deserialize(data, Long.class);
	}

	protected void loadAndAddMetaServerAssignmentWatcher(MetaServerAssignmentChangedWatcher watcher) throws Exception {
		m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
		m_metaServerAssignmentHolder.reload();
	}

	protected void loadAndAddLeaderMetaWatcher(Watcher watcher, final Event event) throws Exception {
		byte[] data = null;
		if (watcher == null) {
			data = m_zkClient.get().getData().forPath(ZKPathUtils.getMetaInfoZkPath());
		} else {
			data = m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getMetaInfoZkPath());
		}

		MetaInfo metaInfo = ZKSerializeUtils.deserialize(data, MetaInfo.class);
		Meta meta = m_leaderMetaFetcher.fetchMetaInfo(metaInfo);

		if (meta != null && (m_metaHolder.getMeta() == null || meta.getVersion() != m_metaHolder.getMeta().getVersion())) {
			m_metaHolder.setMeta(meta);
			log.info("[{}]Fetched meta from leader(endpoint={}:{},version={})", role(), metaInfo.getHost(),
			      metaInfo.getPort(), meta.getVersion());
		} else if (meta == null) {
			delayRetry(event, m_scheduledExecutor, new Task() {

				@Override
				public void run() {
					try {
						loadAndAddLeaderMetaWatcher(null, event);
					} catch (Exception e) {
						log.error("[{}]Exception occurred while loading meta from leader.", role(), e);
					}
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

	protected class BaseMetaWatcher extends BaseEventBasedZkWatcher {

		private ClusterStateHolder m_clusterStateHolder;

		private com.ctrip.hermes.metaserver.event.Event m_event;

		protected BaseMetaWatcher(com.ctrip.hermes.metaserver.event.Event event) {
			super(event.getEventBus(), event.getVersion(), org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
			m_clusterStateHolder = event.getStateHolder();
			m_event = event;
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			fetchBaseMetaAndCheckStateChange(m_event);
		}

		private void fetchBaseMetaAndCheckStateChange(final com.ctrip.hermes.metaserver.event.Event event) {
			try {
				Long baseMetaVersion = loadAndAddBaseMetaWatcher(this, m_clusterStateHolder, false);
				log.info("[{}]BaseMeta changed(version:{}).", role(), baseMetaVersion);

				fetchBaseMetaAndHandleChanged(m_clusterStateHolder);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while fetchBaseMetaAndCheckStateChange, will retry.", role(), e);

				delayRetry(event, m_scheduledExecutor, new Task() {

					@Override
					public void run() {
						fetchBaseMetaAndCheckStateChange(event);
					}
				});
			}
		}

	}

	private class LeaderMetaChangedWatcher extends BaseEventBasedZkWatcher {

		private com.ctrip.hermes.metaserver.event.Event m_event;

		protected LeaderMetaChangedWatcher(com.ctrip.hermes.metaserver.event.Event event) {
			super(event.getEventBus(), event.getVersion(), org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
			m_event = event;
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				loadAndAddLeaderMetaWatcher(this, m_event);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while handling leader meta watcher event.", role(), e);
			}
		}

	}

	private class MetaServerAssignmentChangedWatcher extends BaseEventBasedZkWatcher {

		protected MetaServerAssignmentChangedWatcher(com.ctrip.hermes.metaserver.event.Event event) {
			super(event.getEventBus(), event.getVersion(), org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				loadAndAddMetaServerAssignmentWatcher(this);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while handling meta server assignment watcher event.", role(), e);
			}
		}

	}
}
