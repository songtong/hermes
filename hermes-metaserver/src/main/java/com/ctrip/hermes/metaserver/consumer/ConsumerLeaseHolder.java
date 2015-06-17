package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
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
	protected String[] getZkPersistTouchPaths(Tpg tpg) {
		return new String[] { ZKPathUtils.getConsumerLeaseTopicParentZkPath(tpg.getTopic()) };
	}

	@Override
	protected Tpg convertZkPathToKey(String path) {
		return ZKPathUtils.parseConsumerLeaseZkPath(path);
	}

	@Override
	protected Map<String, Map<String, ClientLeaseInfo>> loadExistingLeases() throws Exception {
		// TODO add watcher for root node and all topics node
		CuratorFramework curatorFramework = m_zkClient.getClient();
		String rootPath = ZKPathUtils.getConsumerLeaseRootZkPath();

		Map<String, Map<String, ClientLeaseInfo>> existingLeases = new HashMap<>();

		List<String> topics = curatorFramework.getChildren().forPath(rootPath);
		if (topics != null && !topics.isEmpty()) {
			for (String topic : topics) {
				List<String> partitions = curatorFramework.getChildren().forPath(ZKPaths.makePath(rootPath, topic));

				if (partitions != null && !partitions.isEmpty()) {
					for (String partition : partitions) {

						List<String> groups = curatorFramework.getChildren().forPath(
						      ZKPaths.makePath(rootPath, topic, partition));

						if (groups != null && !groups.isEmpty()) {
							for (String group : groups) {
								String path = ZKPaths.makePath(rootPath, topic, partition, group);
								byte[] data = curatorFramework.getData().forPath(path);
								if (data != null && data.length != 0) {
									existingLeases.put(path, deserializeExistingLeases(data));
								}
							}
						}
					}
				}
			}
		}

		return existingLeases;
	}
}
