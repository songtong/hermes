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
import com.ctrip.hermes.portal.service.dashboard.DefaultDashboardService;

@Path("/offset")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SitemonResource {
	private DefaultDashboardService m_service = PlexusComponentLocator.lookup(DefaultDashboardService.class);

	@GET
	@Path("/lag")
	public Response getTopicsTag(@QueryParam("storageType") String storageType) {
		if (storageType != null) {
			if (storageType.equals(Storage.MYSQL)) {
				
			} else {
				Map<String, Long> offsets = m_service.findKafkaTopicsOffset();
			}
		} else {
			
		}
		
		return Response.status(Status.OK).build();
	}
}
