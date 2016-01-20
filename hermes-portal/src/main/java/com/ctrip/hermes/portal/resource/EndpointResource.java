package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaservice.service.EndpointService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/endpoints/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class EndpointResource {

	private static final Logger logger = LoggerFactory.getLogger(EndpointResource.class);

	private EndpointService endpointService = PlexusComponentLocator.lookup(EndpointService.class);

	@GET
	public Response getEndpoints() {
		List<Endpoint> endpoints = new ArrayList<Endpoint>(endpointService.getEndpoints().values());
		Collections.sort(endpoints, new Comparator<Endpoint>() {
			@Override
			public int compare(Endpoint o1, Endpoint o2) {
				return o1.getType().compareTo(o2.getType());
			}
		});
		return Response.status(Status.OK).entity(endpoints).build();
	}

	@POST
	public Response addEndpoint(String content) {
		logger.info("Add endpoint: " + content);

		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Endpoint endpoint = null;
		try {
			endpoint = JSON.parseObject(content, Endpoint.class);
		} catch (Exception e) {
			logger.error("Parse consumer failed, content: {}", content, e);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		if (endpointService.getEndpoints().containsKey(endpoint.getId())) {
			throw new RestException(String.format("Endpoint %s already exists.", endpoint.getId()), Status.CONFLICT);
		}

		try {
			endpointService.addEndpoint(endpoint);
			return Response.status(Status.CREATED).build();
		} catch (Exception e) {
			logger.error("Add endpoint failed.", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
	}

	@DELETE
	@Path("{id}")
	public Response deleteEndpoint(@PathParam("id") String id) {
		logger.info("Delete endpoint: {}", id);
		try {
			endpointService.deleteEndpoint(id);
		} catch (Exception e) {
			logger.warn("Delete endpoint failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}
}
