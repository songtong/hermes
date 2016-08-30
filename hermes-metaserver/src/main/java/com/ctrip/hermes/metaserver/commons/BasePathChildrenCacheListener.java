package com.ctrip.hermes.metaserver.commons;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventBus.Task;
import com.ctrip.hermes.metaserver.event.Guard;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BasePathChildrenCacheListener implements PathChildrenCacheListener {

	private static final Logger log = LoggerFactory.getLogger(BasePathChildrenCacheListener.class);

	protected EventBus m_eventBus;

	protected long m_version;

	protected Guard m_guard;

	protected ClusterStateHolder m_clusterStateHolder;

	private ListenerContainer<PathChildrenCacheListener> m_listenerContainer;

	protected BasePathChildrenCacheListener(long version, ListenerContainer<PathChildrenCacheListener> listenerContainer) {
		m_version = version;
		m_eventBus = PlexusComponentLocator.lookup(EventBus.class);
		m_guard = PlexusComponentLocator.lookup(Guard.class);
		m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);
		m_listenerContainer = listenerContainer;
	}

	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED
		      || event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED
		      || event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
			m_eventBus.submit(m_version, new Task() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					try {
						childChanged();
					} finally {
						log.info("PathChildrenCacheListener handle childChanged cost {}ms(name:{}, version:{}).",
						      (System.currentTimeMillis() - start), getName(), m_version);
					}
				}

				@Override
				public void onGuardNotPass() {
					removeListener();
				}
			});
		}
	}

	private void removeListener() {
		log.debug("PathChildrenCacheListener invalidate, will remove from pathChildrenCache(name:{}, version:{})",
		      getName(), m_version);
		m_listenerContainer.removeListener(this);
	}

	protected abstract void childChanged();

	protected abstract String getName();

}
