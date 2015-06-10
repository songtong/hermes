package com.ctrip.hermes.metaserver.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class NodeStatusHolder {

	private AtomicBoolean m_hasLeadership = new AtomicBoolean(false);

	public void takeLeadership() {
		m_hasLeadership.set(true);
	}

	public void loseLeadership() {
		m_hasLeadership.set(false);
	}

	public boolean isResponseFor(String topicName) {
		// TODO
		return true;
	}

}
