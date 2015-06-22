package com.ctrip.hermes.metaserver.broker;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerAssigner {

	void stop(ClusterStateHolder stateHolder);

	void start(ClusterStateHolder stateHolder);

}
