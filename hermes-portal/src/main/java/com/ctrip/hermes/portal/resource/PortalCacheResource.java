package com.ctrip.hermes.portal.resource;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.CacheDalService;
import com.google.common.cache.CacheStats;

@Path("/cache/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PortalCacheResource {

	private static final Logger logger = LoggerFactory.getLogger(PortalCacheResource.class);

	private CacheDalService cacheService = PlexusComponentLocator.lookup(CacheDalService.class);

	@GET
	@Path("stats")
	public Response getStats() {
		Map<String, CacheStats> stats = cacheService.getCacheStats();
		Map<String, String> result = new HashMap<String, String>();
		for (Map.Entry<String, CacheStats> entry : stats.entrySet()) {
			result.put(entry.getKey(), entry.getValue().toString());
		}
		return Response.status(Status.OK).entity(result).build();
	}

	@POST
	@Path("invalidateAll")
	public Response invalidateAll() {
		logger.info("Invalidate all cache");
		cacheService.invalidateAll();
		return Response.status(Status.OK).build();
	}
}
