package com.ctrip.hermes.rest.common;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;
import com.dianping.cat.Cat;
import com.yammer.metrics.Metrics;

public class GatewayListener implements ServletContextListener {

	private SubscriptionRegisterService subsribeRegistry = PlexusComponentLocator
	      .lookup(SubscriptionRegisterService.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		File catConfigFile = new File("/opt/ctrip/data/cat/client.xml");
		if (!catConfigFile.isFile() || !catConfigFile.canRead()) {
			try {
				throw new IllegalStateException(
						  String.format("Cat config file %s not found", catConfigFile.getCanonicalPath()));
			} catch (IOException e) {
				// todo: handle exception
				e.printStackTrace();
			}
		}
		Cat.initialize(catConfigFile);

		subsribeRegistry.start();
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		subsribeRegistry.stop();

		Metrics.defaultRegistry().shutdown();
		
	}

}
