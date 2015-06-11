package com.ctrip.hermes.metaserver.cluster;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ClusterStateChangeListener {

	void notLeader(ClusterStateHolder stateHolder);

	void isLeader(ClusterStateHolder stateHolder);

}
