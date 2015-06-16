package com.ctrip.hermes.metaserver.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseHolder.class)
public class ConsumerLeaseHolder extends BaseLeaseHolder<Tpg> {

	@Override
	protected String convertKeyToZkPath(Tpg tpg) {
		return ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
	}

	@Override
	protected String[] getZkTouchPaths(Tpg tpg) {
		return new String[] { ZKPathUtils.getConsumerLeaseTopicParentZkPath(tpg.getTopic()) };
	}

	@Override
	protected Tpg convertZkPathToKey(String path) {
		return ZKPathUtils.parseConsumerLeaseZkPath(path);
	}

	@Override
	protected List<String> getAllLeavesPaths(CuratorWatcher rootPathWatcher) throws Exception {
		String rootPath = ZKPathUtils.getConsumerLeaseRootZkPath();
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
						// TODO add partition watcher in case new consumer group connected
						List<String> groups = curatorFramework.getChildren().forPath(
						      ZKPaths.makePath(rootPath, topic, partition));

						if (groups != null && !groups.isEmpty()) {
							for (String group : groups) {
								paths.add(ZKPaths.makePath(rootPath, topic, partition, group));
							}
						}
					}
				}
			}
		}

		return paths;
	}
}
