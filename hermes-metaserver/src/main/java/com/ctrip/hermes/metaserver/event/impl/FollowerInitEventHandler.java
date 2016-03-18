package com.ctrip.hermes.metaserver.event.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.BaseEventBasedZkWatcher;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.Guard;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
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
	private MetaHolder m_metaHolder;

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private LeaderMetaFetcher m_leaderMetaFetcher;

	private ScheduledExecutorService m_scheduledExecutor;

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
		loadAndAddLeaderMetaWatcher(new LeaderMetaChangedWatcher(event.getEventBus(), event.getVersion()),
		      event.getEventBus(), event.getVersion());

		loadAndAddMetaServerAssignmentWatcher(new MetaServerAssignmentChangedWatcher(event.getEventBus(),
		      event.getVersion()));
	}

	private void loadAndAddMetaServerAssignmentWatcher(MetaServerAssignmentChangedWatcher watcher) throws Exception {
		m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
		m_metaServerAssignmentHolder.reload();
	}

	private void loadAndAddLeaderMetaWatcher(Watcher watcher, final EventBus eventBus, final long version)
	      throws Exception {
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
			log.info("Fetched meta from leader(endpoint={}:{},version={})", metaInfo.getHost(), metaInfo.getPort(),
			      meta.getVersion());
		} else if (meta == null) {
			delayRetry(eventBus, version);
		}
	}

	private void delayRetry(final EventBus eventBus, final long version) {
		m_scheduledExecutor.schedule(new Runnable() {

			@Override
			public void run() {
				if (version == PlexusComponentLocator.lookup(Guard.class).getVersion()) {
					eventBus.getExecutor().submit(new Runnable() {

						@Override
						public void run() {
							try {
								loadAndAddLeaderMetaWatcher(null, eventBus, version);
							} catch (Exception e) {
								log.error("Exception occurred while loading meta from leader.", e);
							}
						}
					});
				}
			}
		}, 2, TimeUnit.SECONDS);
	}

	@Override
	protected Role role() {
		return Role.FOLLOWER;
	}

	private class LeaderMetaChangedWatcher extends BaseEventBasedZkWatcher {

		protected LeaderMetaChangedWatcher(EventBus eventBus, long version) {
			super(eventBus, version, org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				loadAndAddLeaderMetaWatcher(this, m_eventBus, m_version);
			} catch (Exception e) {
				log.error("Exception occurred while handling leader meta watcher event.", e);
			}
		}

	}

	private class MetaServerAssignmentChangedWatcher extends BaseEventBasedZkWatcher {

		protected MetaServerAssignmentChangedWatcher(EventBus eventBus, long version) {
			super(eventBus, version, org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				loadAndAddMetaServerAssignmentWatcher(this);
			} catch (Exception e) {
				log.error("Exception occurred while handling meta server assignment watcher event.", e);
			}
		}

	}
}
