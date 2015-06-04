package com.ctrip.hermes.rest.common;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.SubscribeRegistry;

public class GatewayListener implements ServletContextListener {

	private SubscribeRegistry subsribeRegistry = PlexusComponentLocator.lookup(SubscribeRegistry.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		subsribeRegistry.start();
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		subsribeRegistry.stop();
	}

}
