package com.ctrip.hermes.metaserver.cluster;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ClusterStateServletListener implements ServletContextListener {
	private static final Logger log = LoggerFactory.getLogger(ClusterStateServletListener.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		try {
			PlexusComponentLocator.lookup(ClusterStateHolder.class).start();
		} catch (Exception e) {
			log.error("Failed to start ClusterStatusHolder.", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		try {
			PlexusComponentLocator.lookup(ClusterStateHolder.class).close();
		} catch (Exception e) {
			log.error("Failed to close ClusterStatusHolder.", e);
			throw new RuntimeException(e);
		}
	}

}
