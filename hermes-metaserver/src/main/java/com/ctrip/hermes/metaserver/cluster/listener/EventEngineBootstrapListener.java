package com.ctrip.hermes.metaserver.cluster.listener;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.cluster.ClusterStateChangeListener;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.EventEngine;
import com.ctrip.hermes.metaserver.event.FollowerEventEngine;
import com.ctrip.hermes.metaserver.event.LeaderEventEngine;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateChangeListener.class, value = "EventEngineBootstrapListener")
public class EventEngineBootstrapListener implements ClusterStateChangeListener {
	private static final Logger log = LoggerFactory.getLogger(EventEngineBootstrapListener.class);

	private AtomicReference<EventEngine> m_leaderEventEngine = new AtomicReference<>();

	private AtomicReference<EventEngine> m_followerEventEngine = new AtomicReference<>();

	@Override
	public void notLeader(ClusterStateHolder stateHolder) {
		EventEngine leaderEventEngine = m_leaderEventEngine.getAndSet(null);
		if (leaderEventEngine != null) {
			stopEventEngineQuietly(leaderEventEngine);
		}

		m_followerEventEngine.set(new FollowerEventEngine());
		try {
			m_followerEventEngine.get().start(stateHolder);
		} catch (Exception e) {
			// TODO maybe stop the world
			log.error("Exception occurred while starting follower event engine.", e);
		}
	}

	@Override
	public void isLeader(ClusterStateHolder stateHolder) {
		EventEngine followerEventEngine = m_followerEventEngine.getAndSet(null);
		if (followerEventEngine != null) {
			stopEventEngineQuietly(followerEventEngine);
		}

		m_leaderEventEngine.set(new LeaderEventEngine());
		try {
			m_leaderEventEngine.get().start(stateHolder);
		} catch (Exception e) {
			// TODO maybe stop the world
			log.error("Exception occurred while starting leader event engine.", e);
		}
	}

	private void stopEventEngineQuietly(EventEngine eventEngine) {
		try {
			eventEngine.stop();
		} catch (Exception e) {
			log.warn("Exception occurred while stopping event engine", e);
		}
	}
}
