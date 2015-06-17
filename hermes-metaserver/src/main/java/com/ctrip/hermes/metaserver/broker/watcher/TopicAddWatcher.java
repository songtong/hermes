package com.ctrip.hermes.metaserver.broker.watcher;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.commons.BaseZkWatcher;
import com.ctrip.hermes.metaservice.zk.ZKClient;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class TopicAddWatcher extends BaseZkWatcher {
	private final static Logger log = LoggerFactory.getLogger(TopicAddWatcher.class);

	private BrokerLeaseHolder m_leaseHolder;

	public TopicAddWatcher(ExecutorService executorService, BrokerLeaseHolder leaseHolder) {
		super(executorService, EventType.NodeChildrenChanged);
		m_leaseHolder = leaseHolder;
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		String path = event.getPath();
		try {
			CuratorFramework curatorFramework = PlexusComponentLocator.lookup(ZKClient.class).getClient();

			List<String> topics = curatorFramework.getChildren().usingWatcher(this).forPath(path);

			for (String topic : topics) {
				if (!m_leaseHolder.containsTopic(topic)) {

				}
			}

		} catch (Exception e) {
			log.error("", e);
		}
	}
}
