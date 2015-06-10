package com.ctrip.hermes.metaserver.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.zk.ZKClient;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStatusHolder.class)
public class ClusterStatusHolder {

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZKClient m_client;

	private LeaderLatch m_leaderLatch;

	private AtomicBoolean m_hasLeadership = new AtomicBoolean(false);

	public boolean hasLeadership() {
		return m_hasLeadership.get();
	}

	public void start() throws Exception {
		m_leaderLatch = new LeaderLatch(m_client.getClient(), m_config.getMetaServerLeaderElectionZkPath(),
		      m_config.getMetaServerName());

		m_leaderLatch.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
				m_hasLeadership.set(false);
				System.out.println("not leader");
			}

			@Override
			public void isLeader() {
				m_hasLeadership.set(true);
				System.out.println("is leader");
			}
		}, Executors.newSingleThreadExecutor(HermesThreadFactory.create("LeaderLatchListenerPool", true)));

		m_leaderLatch.start();
	}

	public void close() throws Exception {
		m_leaderLatch.close();
	}

}
