package com.ctrip.hermes.metaserver.meta;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.Watcher;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.commons.WatcherGuard;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.watcher.BrokerLeaseWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.MetaServerListWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.MetaVersionWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.TopicWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.ZkReader;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaUpdater.class, value = BuildConstants.LEADER)
public class LeaderMetaUpdater implements MetaUpdater, Initializable {

	private final static Logger log = LoggerFactory.getLogger(LeaderMetaUpdater.class);

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private WatcherGuard m_watcherGuard;

	@Inject
	private ZkReader m_zkReader;

	@Inject
	private MetaLoader m_metaLoader;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZookeeperService m_zkService;

	private ExecutorService m_watcherExecutor;

	@Override
	public synchronized void stop(ClusterStateHolder stateHolder) {
		m_watcherGuard.updateVersion();
	}

	@Override
	public synchronized void start(ClusterStateHolder stateHolder) {
		try {
			long newMetaVersion = loadMeta();
			saveMetaVersionToZk(newMetaVersion);
			addMetaVersionWatcher();
			addTopicWatchers();
			addMetaServerListWatcher();
		} catch (Exception e) {
			log.error("Error add watcher to zk", e);
		}
	}

	private void saveMetaVersionToZk(long newMetaVersion) throws Exception {
		MetaInfo metaInfo = new MetaInfo(m_config.getMetaServerHost(), m_config.getMetaServerPort(), newMetaVersion);
		m_zkService.persist(ZKPathUtils.getMetaInfoZkPath(), ZKSerializeUtils.serialize(metaInfo));
	}

	private long loadMeta() throws Exception {
		Meta mergedMeta = m_metaLoader.load();
		MetaInfo curMetaInfo = ZKSerializeUtils.deserialize(
		      m_zkClient.getClient().getData().forPath(ZKPathUtils.getMetaInfoZkPath()), MetaInfo.class);

		long newMetaVersion = System.currentTimeMillis();
		// may be same due to different machine time
		if (curMetaInfo != null && curMetaInfo.getTimestamp() == newMetaVersion) {
			newMetaVersion++;
		}

		// bump version to make new meta effective
		mergedMeta.setVersion(newMetaVersion);
		m_metaHolder.setMeta(mergedMeta);

		return newMetaVersion;
	}

	private void addMetaServerListWatcher() throws Exception {
		String path = ZKPathUtils.getMetaServersZkPath();
		Watcher watcher = new MetaServerListWatcher(m_watcherGuard.getVersion(), m_watcherGuard, m_watcherExecutor);
		m_zkClient.getClient().getChildren().usingWatcher(watcher).forPath(path);
	}

	private void addTopicWatchers() throws Exception {
		List<String> topics = m_zkReader.listTopics();

		// watch every topic node
		for (String topic : topics) {
			String path = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic);
			Watcher watcher = new TopicWatcher(m_watcherGuard.getVersion(), m_watcherGuard, m_watcherExecutor);
			m_zkClient.getClient().getData().usingWatcher(watcher).forPath(path);
		}
		// watch topic's parent node to add watcher to newly added topic
		String path = ZKPathUtils.getBrokerLeaseRootZkPath();
		Watcher watcher = new BrokerLeaseWatcher(m_watcherGuard.getVersion(), m_watcherGuard, m_watcherExecutor, topics);
		m_zkClient.getClient().getChildren().usingWatcher(watcher).forPath(path);
	}

	private void addMetaVersionWatcher() throws Exception {
		String path = ZKPathUtils.getBaseMetaVersionZkPath();
		Watcher watcher = new MetaVersionWatcher(m_watcherGuard.getVersion(), m_watcherGuard, m_watcherExecutor);
		m_zkClient.getClient().getData().usingWatcher(watcher).forPath(path);
	}

	@Override
	public void initialize() throws InitializationException {
		m_watcherExecutor = Executors.newFixedThreadPool(1, HermesThreadFactory.create("MetaWatcher", true));
	}
}
