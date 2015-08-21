package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.DefaultPortalMetaService;
import com.ctrip.hermes.metaservice.service.PortalMetaService;

@Path("/metaserver/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PortalMetaServerResource {

	private PortalMetaService metaService = PlexusComponentLocator.lookup(PortalMetaService.class,
	      DefaultPortalMetaService.ID);

	@GET
	@Path("kafka/zookeeper")
	public String getZookeeperList() {
		return metaService.getZookeeperList();
	}

	@GET
	@Path("kafka/brokers")
	public String getKafkaBrokerList() {
		return metaService.getKafkaBrokerList();
	}
}
