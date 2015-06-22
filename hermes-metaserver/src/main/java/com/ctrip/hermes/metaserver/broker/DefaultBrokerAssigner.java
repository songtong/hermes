package com.ctrip.hermes.metaserver.broker;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssigner.class)
public class DefaultBrokerAssigner implements BrokerAssigner {

	@Override
	public void stop(ClusterStateHolder stateHolder) {
		// TODO Auto-generated method stub

	}

	@Override
	public void start(ClusterStateHolder stateHolder) {
		// TODO Auto-generated method stub

	}

}
