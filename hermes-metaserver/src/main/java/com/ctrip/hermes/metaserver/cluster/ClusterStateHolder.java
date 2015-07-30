package com.ctrip.hermes.metaserver.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaservice.zk.ZKClient;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateHolder.class)
public class ClusterStateHolder {

	private static final Logger log = LoggerFactory.getLogger(ClusterStateHolder.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZKClient m_client;

	@Inject
	private ClusterStateChangeListenerContainer m_listenerContainer;

	private LeaderLatch m_leaderLatch;

	private AtomicBoolean m_hasLeadership = new AtomicBoolean(false);

	private AtomicReference<HostPort> m_leader = new AtomicReference<>(null);

	public void setHasLeadership(boolean hasLeadership) {
		m_hasLeadership.set(hasLeadership);
	}

	public boolean hasLeadership() {
		return m_hasLeadership.get();
	}

	public void start() throws Exception {
		m_leaderLatch = new LeaderLatch(m_client.get(), m_config.getMetaServerLeaderElectionZkPath(),
		      m_config.getMetaServerName());

		m_leaderLatch.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
				log.info("Become follower");
				m_hasLeadership.set(false);
				m_leader.set(fetcheLeaderInfoFromZk());
				m_listenerContainer.notLeader(ClusterStateHolder.this);
			}

			@Override
			public void isLeader() {
				log.info("Become leader");
				m_hasLeadership.set(true);
				m_leader.set(fetcheLeaderInfoFromZk());
				m_listenerContainer.isLeader(ClusterStateHolder.this);
			}
		}, Executors.newSingleThreadExecutor(HermesThreadFactory.create("LeaderLatchListenerPool", true)));

		// call notLeader before start, since if this is not leader, it won't trigger notLeader on start
		m_listenerContainer.notLeader(this);

		m_leaderLatch.start();
	}

	public void close() throws Exception {
		m_leaderLatch.close();
	}

	public HostPort getLeader() {
		if (m_leader.get() == null) {
			m_leader.set(fetcheLeaderInfoFromZk());
		}
		return m_leader.get();

	}

	private HostPort fetcheLeaderInfoFromZk() {
		try {
			Participant leader = m_leaderLatch.getLeader();
			return JSON.parseObject(leader.getId(), HostPort.class);
		} catch (Exception e) {
			log.error("Failed to fetch leader info from zk.", e);
		}

		return null;
	}
}
