package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.broker.watcher.BrokerLeaseAddedWatcher;
import com.ctrip.hermes.metaserver.broker.watcher.BrokerLeaseChangedWatcher;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

	private ExecutorService m_watcherExecutorService;

	private Set<String> m_watchedTopics = new HashSet<>();

	private BrokerLeaseChangedWatcher m_brokerLeaseChangedWatcher;

	private BrokerLeaseAddedWatcher m_brokerLeaseAddedWatcher;

	@Override
	protected String convertKeyToZkPath(Pair<String, Integer> topicPartition) {
		return ZKPathUtils.getBrokerLeaseZkPath(topicPartition.getKey(), topicPartition.getValue());
	}

	@Override
	protected String[] getZkPersistTouchPaths(Pair<String, Integer> topicPartition) {
		return new String[] { ZKPathUtils.getBrokerLeaseTopicParentZkPath(topicPartition.getKey()) };
	}

	@Override
	protected Pair<String, Integer> convertZkPathToKey(String path) {
		return ZKPathUtils.parseBrokerLeaseZkPath(path);
	}

	@Override
	protected Map<String, Map<String, ClientLeaseInfo>> loadExistingLeases() throws Exception {
		CuratorFramework client = m_zkClient.get();

		Map<String, Map<String, ClientLeaseInfo>> existingLeases = new HashMap<>();

		List<String> topics = client.getChildren()//
		      .usingWatcher(m_brokerLeaseAddedWatcher)//
		      .forPath(ZKPathUtils.getBrokerLeaseRootZkPath());
		m_brokerLeaseAddedWatcher.addWatchedPath(ZKPathUtils.getBrokerLeaseRootZkPath());

		if (topics != null && !topics.isEmpty()) {
			for (String topic : topics) {
				Map<String, Map<String, ClientLeaseInfo>> topicExistingLeases = loadAndWatchTopicExistingLeases(topic);
				existingLeases.putAll(topicExistingLeases);
			}
		}

		return existingLeases;
	}

	public Map<String, Map<String, ClientLeaseInfo>> loadAndWatchTopicExistingLeases(String topic) throws Exception {
		Map<String, Map<String, ClientLeaseInfo>> topicExistingLeases = new HashMap<>();

		CuratorFramework client = m_zkClient.get();
		String topicPath = ZKPaths.makePath(ZKPathUtils.getBrokerLeaseRootZkPath(), topic);

		client.getData().usingWatcher(m_brokerLeaseChangedWatcher).forPath(topicPath);
		m_brokerLeaseChangedWatcher.addWatchedPath(topicPath);
		addWatchedTopic(topic);

		List<String> partitions = client.getChildren().forPath(topicPath);

		if (partitions != null && !partitions.isEmpty()) {
			for (String partition : partitions) {
				String partitionPath = ZKPaths.makePath(topicPath, partition);
				byte[] data = client.getData().forPath(partitionPath);
				Map<String, ClientLeaseInfo> existingLeases = deserializeExistingLeases(data);
				if (existingLeases != null) {
					topicExistingLeases.put(partitionPath, existingLeases);
				}
			}
		}

		return topicExistingLeases;
	}

	@Override
	protected void doInitialize() {
		m_watcherExecutorService = Executors.newSingleThreadExecutor(HermesThreadFactory.create("BrokerLeaseWatcher",
		      true));
		m_brokerLeaseChangedWatcher = new BrokerLeaseChangedWatcher(m_watcherExecutorService, this);
		m_brokerLeaseAddedWatcher = new BrokerLeaseAddedWatcher(m_watcherExecutorService, this);
	}

	public synchronized boolean topicWatched(String topic) {
		return m_watchedTopics.contains(topic);
	}

	public synchronized void addWatchedTopic(String topic) {
		m_watchedTopics.add(topic);
	}

	public synchronized void removeWatchedTopic(String topic) {
		m_watchedTopics.remove(topic);
	}

}
