package com.ctrip.hermes.metaserver.cluster;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateChangeListenerContainer.class)
public class ClusterStateChangeListenerContainer extends ContainerHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(ClusterStateChangeListenerContainer.class);

	public Map<String, ClusterStateChangeListener> m_listeners = new HashMap<>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, ClusterStateChangeListener> listeners = lookupMap(ClusterStateChangeListener.class);

		if (listeners != null && !listeners.isEmpty()) {
			m_listeners.putAll(listeners);
		}
	}

	public void notLeader(ClusterStateHolder stateHolder) {
		if (!m_listeners.isEmpty()) {
			for (Map.Entry<String, ClusterStateChangeListener> entry : m_listeners.entrySet()) {
				try {
					entry.getValue().notLeader(stateHolder);
				} catch (Exception e) {
					log.error("Exception occured while calling ClusterStateChangeListener's notLeader(listener={}).",
					      entry.getKey(), e);
				}
			}
		}
	}

	public void isLeader(ClusterStateHolder stateHolder) {
		if (!m_listeners.isEmpty()) {
			for (Map.Entry<String, ClusterStateChangeListener> entry : m_listeners.entrySet()) {
				try {
					entry.getValue().isLeader(stateHolder);
				} catch (Exception e) {
					log.error("Exception occured while calling ClusterStateChangeListener's isLeader(listener={}).",
					      entry.getKey(), e);
				}
			}
		}
	}

}
