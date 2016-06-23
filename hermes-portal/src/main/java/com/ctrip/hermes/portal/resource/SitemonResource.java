package com.ctrip.hermes.portal.resource;

import java.util.Map;

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
import com.ctrip.hermes.portal.service.dashboard.DashboardService;

@Path("/sitemon")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SitemonResource {
	private DashboardService m_service = PlexusComponentLocator.lookup(DashboardService.class);

	@GET
	@Path("/offset/lag")
	public Response getTopicsTag(@QueryParam("storageType") String storageType) {
		Map<String, Long> offsets = null;
		if (storageType != null) {
			if (storageType.equals(Storage.MYSQL)) {
				
			} else {
				offsets = m_service.findKafkaOffsetLags();
			}
		} else {
			
		}
		
		return Response.status(Status.OK).entity(offsets).build();
	}
}
