package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.dashboard.DashboardService;

@Path("/sitemon")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SitemonResource {
	private DashboardService m_service = PlexusComponentLocator.lookup(DashboardService.class);

	@GET
	@Path("/offset/lag")
	public Response getTopicsTag(@QueryParam("storageType") String storageType) {
		if (storageType == null) {
			return Response.status(Status.OK).entity(m_service.getTopicOffsetLags()).build(); 
		}
		
		if (storageType.equals(Storage.KAFKA) || storageType.equals(Storage.MYSQL)) {
			return Response.status(Status.OK).entity(m_service.getTopicOffsetLags().get(storageType)).build();  
		}
		
		throw new RestException("Invalid parameter value for storage-type.", Status.BAD_REQUEST);
		
	}
}
