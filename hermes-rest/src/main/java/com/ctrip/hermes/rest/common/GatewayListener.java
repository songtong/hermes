package com.ctrip.hermes.rest.common;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;

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

		CommandProcessorManager commandProcessorManager = PlexusComponentLocator.lookup(CommandProcessorManager.class);
		commandProcessorManager.stop();
		
		EndpointClient endpointClient = PlexusComponentLocator.lookup(EndpointClient.class);
		endpointClient.close();
	
		HermesMetricsRegistry.close();
	}

}
