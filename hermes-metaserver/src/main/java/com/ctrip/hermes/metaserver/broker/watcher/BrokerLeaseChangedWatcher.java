package com.ctrip.hermes.metaserver.broker.watcher;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.BaseZkWatcher;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerLeaseChangedWatcher extends BaseZkWatcher {

	private final static Logger log = LoggerFactory.getLogger(BrokerLeaseChangedWatcher.class);

	private BrokerLeaseHolder m_leaseHolder;

	public BrokerLeaseChangedWatcher(ExecutorService executorService, BrokerLeaseHolder leaseHolder) {
		super(executorService, EventType.NodeDataChanged, EventType.NodeDeleted);
		m_leaseHolder = leaseHolder;
	}

	@Override
	protected void doProcess(WatchedEvent event) {
		String path = event.getPath();
		try {
			String topic = ZKPathUtils.lastSegment(path);

			EventType type = event.getType();
			if (type == EventType.NodeDataChanged) {
				log.info("Broker lease changed for topic {}", topic);

				Map<String, Map<String, ClientLeaseInfo>> topicExistingLeases = m_leaseHolder
				      .loadAndWatchTopicExistingLeases(topic);

				m_leaseHolder.updateContexts(topicExistingLeases);
			} else {
				log.info("Broker lease removed for topic {}", topic);

				// we don't need to remove it from the lease holder, since it will be removed by housekeeper
				m_leaseHolder.removeWatchedTopic(topic);
			}

		} catch (Exception e) {
			log.error("Exception occured while handling broker lease topic changed check.", e);
		}
	}

}
