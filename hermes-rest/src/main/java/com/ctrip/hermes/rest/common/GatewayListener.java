package com.ctrip.hermes.rest.common;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;
import com.yammer.metrics.Metrics;

public class GatewayListener implements ServletContextListener {

	private SubscriptionRegisterService subsribeRegistry = PlexusComponentLocator
	      .lookup(SubscriptionRegisterService.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		subsribeRegistry.start();
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		subsribeRegistry.stop();

		Metrics.defaultRegistry().shutdown();
		
	}

}
