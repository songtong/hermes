package com.ctrip.hermes.metaserver.cluster.listener;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.broker.BrokerAssigner;
import com.ctrip.hermes.metaserver.cluster.ClusterStateChangeListener;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateChangeListener.class, value = "BrokerAssignerBootstrapListener")
public class BrokerAssignerBootstrapListener implements ClusterStateChangeListener {

	@Inject
	private BrokerAssigner m_brokerAssigner;

	@Override
	public void notLeader(ClusterStateHolder stateHolder) {
		m_brokerAssigner.stop(stateHolder);
	}

	@Override
	public void isLeader(ClusterStateHolder stateHolder) {
		m_brokerAssigner.start(stateHolder);
	}

}
