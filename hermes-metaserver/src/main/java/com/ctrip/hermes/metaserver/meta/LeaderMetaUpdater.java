package com.ctrip.hermes.metaserver.meta;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.meta.watcher.MetaServerListWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.MetaVersionWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.TopicWatcher;
import com.ctrip.hermes.metaserver.meta.watcher.WatcherGuard;
import com.ctrip.hermes.metaserver.meta.watcher.ZkReader;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaUpdater.class, value = BuildConstants.LEADER)
public class LeaderMetaUpdater implements MetaUpdater, Initializable {

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private WatcherGuard m_watcherGuard;

	@Inject
	private ZkReader m_zkReader;

	private ExecutorService m_watcherExecutor;

	@Override
	public synchronized void stop(ClusterStateHolder stateHolder) {
		m_watcherGuard.updateVersion();
	}

	@Override
	public synchronized void start(ClusterStateHolder stateHolder) {
		try {
			addMetaVersionWatcher();
			addTopicWatchers();
			addMetaServerListWatcher();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void addMetaServerListWatcher() throws Exception {
		String path = ZKPathUtils.getMetaServersPath();
		MetaServerListWatcher watcher = new MetaServerListWatcher(m_watcherGuard.getVersion(), m_watcherGuard,
		      m_watcherExecutor);
		m_zkClient.getClient().getChildren().usingWatcher(watcher).forPath(path);
	}

	private void addTopicWatchers() throws Exception {
		List<String> topics = m_zkReader.listTopics();

		for (String topic : topics) {
			String path = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic);
			TopicWatcher watcher = new TopicWatcher(m_watcherGuard.getVersion(), m_watcherGuard, m_watcherExecutor);
			m_zkClient.getClient().getData().usingWatcher(watcher).forPath(path);
		}
	}

	private void addMetaVersionWatcher() throws Exception {
		String path = ZKPathUtils.getMetaVersionPath();
		MetaVersionWatcher watcher = new MetaVersionWatcher(m_watcherGuard.getVersion(), m_watcherGuard,
		      m_watcherExecutor);
		m_zkClient.getClient().getData().usingWatcher(watcher).forPath(path);
	}

	@Override
	public void initialize() throws InitializationException {
		m_watcherExecutor = Executors.newFixedThreadPool(1, HermesThreadFactory.create("ZKWatcher", true));
	}
}
