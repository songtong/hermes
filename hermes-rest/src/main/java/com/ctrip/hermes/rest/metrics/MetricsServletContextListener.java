package com.ctrip.hermes.rest.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

public class MetricsServletContextListener extends MetricsServlet.ContextListener {

	@Override
	protected MetricRegistry getMetricRegistry() {
		return RestMetricsRegistry.getMetricRegistry();
	}

}
