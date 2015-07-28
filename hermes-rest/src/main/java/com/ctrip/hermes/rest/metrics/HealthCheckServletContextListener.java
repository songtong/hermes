package com.ctrip.hermes.rest.metrics;

import javax.servlet.ServletContextEvent;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;

public class HealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

	@Override
	protected HealthCheckRegistry getHealthCheckRegistry() {
		return RestMetricsRegistry.getHealthCheckRegistry();
	}

	@Override
	public void contextInitialized(ServletContextEvent event) {
		super.contextInitialized(event);
		GatewayHealthCheck checker = new GatewayHealthCheck();
		getHealthCheckRegistry().register("metaChecker", checker);
	}
}
