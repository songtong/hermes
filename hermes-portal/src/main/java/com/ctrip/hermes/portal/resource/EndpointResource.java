package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.unidal.tuple.Pair;

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

		Pair<Boolean, String> validate = validateEndpoint(endpoint);
		if (!validate.getKey()) {
			throw new RestException(validate.getValue(), Status.BAD_REQUEST);
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
		} catch (IllegalStateException e) {
			logger.warn("Delete endpoint failed.", e);
			throw new RestException(e.getMessage(), Status.BAD_REQUEST);
		} catch (Exception e) {
			logger.warn("Delete endpoint failed.", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("update")
	public Response updateEndpoint(String content) {
		logger.info("Update endpoint with payload {}", content);

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

		try {
			endpointService.updateEndpoint(endpoint);
			return Response.status(Status.OK).build();
		} catch (IllegalStateException e) {
			logger.warn("Update endpoint failed.", e);
			throw new RestException(e.getMessage(), Status.BAD_REQUEST);
		} catch (Exception e) {
			logger.error("Update endpoint failed.", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}

	}

	@GET
	@Path("brokerGroups")
	public Response getBokerGroups() {
		List<Endpoint> endpoints = new ArrayList<Endpoint>(endpointService.getEndpoints().values());
		Set<String> brokerGroups = new HashSet<>();
		for (Endpoint endpoint : endpoints) {
			brokerGroups.add(endpoint.getGroup());
		}
		return Response.status(Status.OK).entity(brokerGroups).build();
	}

	private Pair<Boolean, String> validateEndpoint(Endpoint e) {
		if (StringUtils.isEmpty(e.getId())) {
			return new Pair<Boolean, String>(false, "Endpoint id can not be null!");
		}

		return new Pair<Boolean, String>(true, null);

	}
}
