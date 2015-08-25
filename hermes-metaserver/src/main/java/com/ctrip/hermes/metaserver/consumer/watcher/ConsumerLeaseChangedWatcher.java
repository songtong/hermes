package com.ctrip.hermes.metaserver.consumer.watcher;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.commons.BaseZKWatcher;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ConsumerLeaseChangedWatcher extends BaseZKWatcher {

	private final static Logger log = LoggerFactory.getLogger(ConsumerLeaseChangedWatcher.class);

	private ConsumerLeaseHolder m_leaseHolder;

	private Set<String> m_watchedPaths = new ConcurrentSkipListSet<>();

	public ConsumerLeaseChangedWatcher(ExecutorService executorService, ConsumerLeaseHolder leaseHolder) {
		super(executorService, EventType.NodeDataChanged, EventType.NodeDeleted);
		m_leaseHolder = leaseHolder;
	}

	public void addWatchedPath(String watchedPath) {
		m_watchedPaths.add(watchedPath);
	}

	public void removeWatchedPath(String watchedPath) {
		m_watchedPaths.remove(watchedPath);
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		String path = event.getPath();
		try {
			if (!StringUtils.isBlank(path)) {
				String topic = ZKPathUtils.lastSegment(path);

				EventType type = event.getType();
				if (type == EventType.NodeDataChanged) {
					if (log.isDebugEnabled()) {
						log.info("Consumer lease changed for topic {}", topic);
					}

					Map<String, Map<String, ClientLeaseInfo>> topicExistingLeases = m_leaseHolder
					      .loadAndWatchTopicExistingLeases(topic);

					m_leaseHolder.updateContexts(topicExistingLeases);
				} else {
					log.info("Consumer lease removed for topic {}", topic);

					// we don't need to remove it from the lease holder, since it will be removed by housekeeper
					m_leaseHolder.removeWatchedTopic(topic);
					removeWatchedPath(path);
				}
			} else {
				CuratorFramework client = PlexusComponentLocator.lookup(ZKClient.class).get();
				for (String watchedPath : m_watchedPaths) {
					client.getData().usingWatcher(this).forPath(watchedPath);
				}
			}

		} catch (Exception e) {
			log.error("Exception occurred while handling consumer lease topic changed check.", e);
		}
	}

}
