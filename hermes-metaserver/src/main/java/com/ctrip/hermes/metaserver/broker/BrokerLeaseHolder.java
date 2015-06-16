package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
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
	protected String[] getZkTouchPaths(Pair<String, Integer> topicPartition) {
		return new String[] { ZKPathUtils.getBrokerLeaseTopicParentZkPath(topicPartition.getKey()) };
	}

	@Override
	protected Pair<String, Integer> convertZkPathToKey(String path) {
		return ZKPathUtils.parseBrokerLeaseZkPath(path);
	}

	@Override
	protected List<String> getAllLeavesPaths(CuratorWatcher rootPathWatcher) throws Exception {
		String rootPath = ZKPathUtils.getBrokerLeaseRootZkPath();
		CuratorFramework curatorFramework = m_zkClient.getClient();

		List<String> paths = new ArrayList<>();

		List<String> topics = null;
		if (rootPathWatcher != null) {
			topics = curatorFramework.getChildren().usingWatcher(rootPathWatcher).forPath(rootPath);
		} else {
			topics = curatorFramework.getChildren().forPath(rootPath);
		}

		if (topics != null && !topics.isEmpty()) {
			for (String topic : topics) {
				List<String> partitions = curatorFramework.getChildren().forPath(ZKPaths.makePath(rootPath, topic));

				if (partitions != null && !partitions.isEmpty()) {
					for (String partition : partitions) {
						paths.add(ZKPaths.makePath(rootPath, topic, partition));
					}
				}
			}
		}

		return paths;
	}
}
