package com.ctrip.hermes.rest.metrics;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;

public class HealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

	@Override
	protected HealthCheckRegistry getHealthCheckRegistry() {
		return RestMetricsRegistry.getInstance().getHealthCheckRegistry();
	}
}
