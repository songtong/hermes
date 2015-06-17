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
import com.ctrip.hermes.metaserver.broker.watcher.LeaseWatcher;
import com.ctrip.hermes.metaserver.broker.watcher.TopicAddWatcher;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

	private ExecutorService m_watcherExecutorService;

	private Set<String> m_topics = new HashSet<>();

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
		// TODO add watcher for root node and all topics node
		CuratorFramework curatorFramework = m_zkClient.getClient();
		String rootPath = ZKPathUtils.getBrokerLeaseRootZkPath();

		Map<String, Map<String, ClientLeaseInfo>> existingLeases = new HashMap<>();

		List<String> topics = curatorFramework.getChildren()
		      .usingWatcher(new TopicAddWatcher(m_watcherExecutorService, this)).forPath(rootPath);
		if (topics != null && !topics.isEmpty()) {
			for (String topic : topics) {
				List<String> partitions = curatorFramework.getChildren().forPath(ZKPaths.makePath(rootPath, topic));

				if (partitions != null && !partitions.isEmpty()) {
					for (String partition : partitions) {
						String path = ZKPaths.makePath(rootPath, topic, partition);
						byte[] data = curatorFramework.getData().usingWatcher(new LeaseWatcher(m_watcherExecutorService))
						      .forPath(path);
						if (data != null && data.length != 0) {
							existingLeases.put(path, deserializeExistingLeases(data));
						}
					}
				}
			}
		}

		return existingLeases;
	}

	@Override
	protected void doInitialize() {
		m_watcherExecutorService = Executors.newSingleThreadExecutor(HermesThreadFactory.create("BrokerLeaseWatcher",
		      true));
	}

	public boolean containsTopic(String topic) {
		// TODO Auto-generated method stub
		return false;
	}

}
