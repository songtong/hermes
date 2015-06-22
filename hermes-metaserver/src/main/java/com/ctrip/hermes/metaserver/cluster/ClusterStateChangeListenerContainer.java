package com.ctrip.hermes.metaserver.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateChangeListenerContainer.class)
public class ClusterStateChangeListenerContainer extends ContainerHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(ClusterStateChangeListenerContainer.class);

	public Map<String, Pair<ClusterStateChangeListener, ExecutorService>> m_listeners = new HashMap<>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, ClusterStateChangeListener> listeners = lookupMap(ClusterStateChangeListener.class);

		if (listeners != null && !listeners.isEmpty()) {
			for (Map.Entry<String, ClusterStateChangeListener> entry : listeners.entrySet()) {
				String threadPoolName = "ClusterStateChange-" + entry.getKey();
				ThreadFactory threadFactory = HermesThreadFactory.create(threadPoolName, true);
				ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
				m_listeners.put(entry.getKey(), new Pair<>(entry.getValue(), executor));
			}
		}
	}

	public void notLeader(final ClusterStateHolder stateHolder) {
		if (!m_listeners.isEmpty()) {
			for (Map.Entry<String, Pair<ClusterStateChangeListener, ExecutorService>> entry : m_listeners.entrySet()) {
				final ClusterStateChangeListener listener = entry.getValue().getKey();
				ExecutorService executor = entry.getValue().getValue();
				final String listenerName = entry.getKey();
				executor.submit(new Runnable() {

					@Override
					public void run() {
						try {
							listener.notLeader(stateHolder);
						} catch (Exception e) {
							log.error("Exception occurred while calling ClusterStateChangeListener's notLeader(listener={}).",
							      listenerName, e);
						}
					}
				});
			}
		}
	}

	public void isLeader(final ClusterStateHolder stateHolder) {
		if (!m_listeners.isEmpty()) {
			for (Map.Entry<String, Pair<ClusterStateChangeListener, ExecutorService>> entry : m_listeners.entrySet()) {
				final ClusterStateChangeListener listener = entry.getValue().getKey();
				ExecutorService executor = entry.getValue().getValue();
				final String listenerName = entry.getKey();
				executor.submit(new Runnable() {

					@Override
					public void run() {
						try {
							listener.isLeader(stateHolder);
						} catch (Exception e) {
							log.error("Exception occurred while calling ClusterStateChangeListener's isLeader(listener={}).",
							      listenerName, e);
						}
					}
				});
			}
		}
	}
}
