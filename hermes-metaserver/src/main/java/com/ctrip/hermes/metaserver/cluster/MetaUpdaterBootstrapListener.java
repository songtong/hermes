package com.ctrip.hermes.metaserver.cluster;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.meta.MetaUpdater;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateChangeListener.class, value = "MetaUpdaterBootstrapListener")
public class MetaUpdaterBootstrapListener implements ClusterStateChangeListener {

	@Inject(value = BuildConstants.LEADER)
	private MetaUpdater m_leaderMetaUpdater;

	@Inject(value = BuildConstants.FOLLOWER)
	private MetaUpdater m_followerMetaUpdater;

	@Override
	public void notLeader(ClusterStateHolder stateHolder) {
		m_leaderMetaUpdater.stop(stateHolder);
		m_followerMetaUpdater.start(stateHolder);
	}

	@Override
	public void isLeader(ClusterStateHolder stateHolder) {
		m_followerMetaUpdater.stop(stateHolder);
		m_leaderMetaUpdater.start(stateHolder);
	}

}
