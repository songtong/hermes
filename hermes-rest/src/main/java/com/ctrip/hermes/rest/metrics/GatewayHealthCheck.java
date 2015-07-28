package com.ctrip.hermes.rest.metrics;

import java.util.List;

import com.codahale.metrics.health.HealthCheck;
import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class GatewayHealthCheck extends HealthCheck {

	private MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);;

	@Override
	protected Result check() throws Exception {
		List<SchemaView> listSchemas = metaService.listSchemas();
		if (listSchemas != null && listSchemas.size() > 0) {
			return Result.healthy();
		} else {
			return Result.unhealthy("Could not get schemas from meta service");
		}
	}

}
