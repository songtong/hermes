package com.ctrip.hermes.metaserver.broker.watcher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.commons.BaseZKWatcher;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaservice.zk.ZKClient;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerLeaseAddedWatcher extends BaseZKWatcher {
	private final static Logger log = LoggerFactory.getLogger(BrokerLeaseAddedWatcher.class);

	@Inject
	private BrokerAssignmentHolder m_brokerAssignment;

	private BrokerLeaseHolder m_leaseHolder;

	public BrokerLeaseAddedWatcher(ExecutorService executorService, BrokerLeaseHolder leaseHolder) {
		super(executorService, EventType.NodeChildrenChanged);
		m_leaseHolder = leaseHolder;
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		String path = event.getPath();
		try {
			CuratorFramework client = PlexusComponentLocator.lookup(ZKClient.class).get();

			List<String> topics = client.getChildren().usingWatcher(this).forPath(path);

			for (String topic : topics) {
				if (!m_leaseHolder.topicWatched(topic)) {
					if (log.isDebugEnabled()) {
						log.debug("Broker lease added for topic {}.", topic);
					}

					Map<String, Map<String, ClientLeaseInfo>> topicExistingLeases = m_leaseHolder
					      .loadAndWatchTopicExistingLeases(topic);

					m_leaseHolder.updateContexts(topicExistingLeases);
				}
			}

		} catch (Exception e) {
			log.error("Exception occurred while handling broker lease added.", e);
		}
	}
}
