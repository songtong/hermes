package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.ctrip.hermes.admin.core.service.StorageService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Path("/metaserver/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PortalMetaServerResource {

	private StorageService dsService = PlexusComponentLocator.lookup(StorageService.class);

	@GET
	@Path("kafka/zookeeper")
	public Response getZookeeperList() {
		return Response.ok().entity(dsService.getKafkaZookeeperList()).build();
	}

	@GET
	@Path("kafka/brokers")
	public String getKafkaBrokerList() {
		return dsService.getKafkaBrokerList();
	}
}
