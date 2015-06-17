package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

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

		List<String> topics = curatorFramework.getChildren().forPath(rootPath);
		if (topics != null && !topics.isEmpty()) {
			for (String topic : topics) {
				List<String> partitions = curatorFramework.getChildren().forPath(ZKPaths.makePath(rootPath, topic));

				if (partitions != null && !partitions.isEmpty()) {
					for (String partition : partitions) {
						String path = ZKPaths.makePath(rootPath, topic, partition);
						byte[] data = curatorFramework.getData().forPath(path);
						existingLeases.put(path, deserializeExistingLeases(data));
					}
				}
			}
		}

		return existingLeases;
	}

}
