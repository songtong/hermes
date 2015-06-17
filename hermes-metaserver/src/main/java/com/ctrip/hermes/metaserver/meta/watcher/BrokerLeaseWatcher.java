package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

public class BrokerLeaseWatcher extends GuardedWatcher {

	private final static Logger log = LoggerFactory.getLogger(BrokerLeaseWatcher.class);

	private Set<String> m_watchedTopics;

	public BrokerLeaseWatcher(int version, WatcherGuard guard, ExecutorService executor, List<String> topics) {
		super(version, guard, executor, EventType.NodeChildrenChanged);
		m_watchedTopics = new HashSet<>(topics);
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		log.info("Topic list updated on ZK");
		try {
			CuratorFramework client = PlexusComponentLocator.lookup(ZKClient.class).getClient();

			String path = ZKPathUtils.getBrokerLeaseRootZkPath();
			List<String> newTopics = client.getChildren().usingWatcher(this).forPath(path);

			List<String> addedTopics = findAddedTopics(newTopics);
			for (String topic : addedTopics) {
				String topicPath = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic);
				Watcher watcher = new TopicWatcher(m_version, m_guard, m_executor);
				client.getData().usingWatcher(watcher).forPath(topicPath);
			}

			m_watchedTopics.clear();
			m_watchedTopics.addAll(newTopics);
		} catch (Exception e) {
			log.error("Error update topic list from ZK", e);
		}
	}

	private List<String> findAddedTopics(List<String> newTopics) {
		List<String> addedTopics = new ArrayList<>();
		if (CollectionUtil.isNotEmpty(newTopics)) {
			for (String topic : newTopics) {
				if (!m_watchedTopics.contains(topic)) {
					addedTopics.add(topic);
				}
			}
		}

		return addedTopics;
	}

}
