package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaserver.commons.GuardedWatcher;
import com.ctrip.hermes.metaserver.commons.WatcherGuard;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

public class TopicWatcher extends GuardedWatcher {

	private final static Logger log = LoggerFactory.getLogger(TopicWatcher.class);

	public TopicWatcher(int version, WatcherGuard guard, ExecutorService executor) {
		super(version, guard, executor, EventType.NodeDataChanged);
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		log.info("topic of path {}'s partition to endpoint mapping has changed", event.getPath());
		try {
			CuratorFramework client = PlexusComponentLocator.lookup(ZKClient.class).getClient();
			MetaHolder metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

			Map<String, Map<Integer, Endpoint>> topicPartitionMap = new HashMap<>();
			String topic = ZKPathUtils.lastSegment(event.getPath());
			client.getData().usingWatcher(this).forPath(ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic));
			topicPartitionMap.put(topic, fetchPartition2Endpoint(topic));
			metaHolder.update(topicPartitionMap);
		} catch (Exception e) {
			log.error("Error update topic of path {}'s partition to endpoint mapping from ZK", event.getPath(), e);
		}
	}

	private Map<Integer, Endpoint> fetchPartition2Endpoint(String topic) throws Exception {
		ZkReader zkReader = PlexusComponentLocator.lookup(ZkReader.class);

		return zkReader.readPartition2Endpoint(topic);
	}

}
