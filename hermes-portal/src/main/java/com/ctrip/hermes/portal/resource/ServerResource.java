package com.ctrip.hermes.portal.resource;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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
import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.service.ServerService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("servers")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ServerResource {

	private static final Logger m_logger = LoggerFactory.getLogger(ServerResource.class);

	private ServerService m_serverService = PlexusComponentLocator.lookup(ServerService.class);

	@GET
	public Response getServers() {
		List<Server> servers = null;
		try {
			servers = m_serverService.findServers();
		} catch (Exception e) {
			m_logger.error("Error in fetching server list from db!", e);
			throw new RestException("Failed to get server list!", Status.INTERNAL_SERVER_ERROR);
		}

		return Response.ok().entity(servers).build();
	}

	@GET
	@Path("{serverName}")
	public Response gerServer(@PathParam("serverName") String serverName) {
		Server server = null;
		try {
			server = m_serverService.findServerByName(serverName);
		} catch (Exception e) {
			m_logger.error("Error in getting server : {} from db!", serverName, e);
			throw new RestException(String.format("Can not get server : %s!", serverName), Status.INTERNAL_SERVER_ERROR);
		}
		if (server == null) {
			throw new RestException(String.format("Can not find server : %s!", serverName), Status.NOT_FOUND);
		}

		return Response.ok().entity(server).build();
	}

	@POST
	public Response addServer(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("Query content is null!", Status.BAD_REQUEST);
		}
		Server server = null;
		try {
			server = JSON.parseObject(content, Server.class);
		} catch (Exception e) {
			m_logger.warn("Can not parse query content:{} to server object!", content, e);
			throw new RestException("Can not parse query content to server object!", Status.BAD_REQUEST);
		}

		Pair<Boolean, String> validate = validateAndTrimServer(server);
		if (!validate.getKey()) {
			throw new RestException(validate.getValue(), Status.BAD_REQUEST);
		}
		
		m_logger.info("Creating new server with payload:{}", content);
		try {
			m_serverService.addServer(server);
		} catch (Exception e) {
			m_logger.error("Error in inserting new server into db!", e);
			throw new RestException("Failed to create new server!", e);
		}

		return Response.status(Status.CREATED).entity(server).build();
	}

	@DELETE
	@Path("{serverName}")
	public Response deleteServer(@PathParam("serverName") String serverName) {
		m_logger.info("Deleting server : {}", serverName);
		try {
			m_serverService.deleteServer(serverName);
		} catch (Exception e) {
			m_logger.error("Deleting server : {} failed!", serverName, e);
			throw new RestException("Deleting server failed!", e);
		}

		return Response.ok().build();
	}

	@PUT
	public Response updateServer(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("Payload can not bu null!", Status.BAD_REQUEST);
		}

		Server server = null;
		try {
			server = JSON.parseObject(content, Server.class);
		} catch (Exception e) {
			m_logger.warn("Can not parse query content:{} to server object!", content, e);
			throw new RestException("Can not parse query content to server object!", Status.BAD_REQUEST);
		}

		Pair<Boolean, String> validate = validateAndTrimServer(server);
		if (!validate.getKey()) {
			throw new RestException(validate.getValue(), Status.BAD_REQUEST);
		}
		
		m_logger.info("Update server : {} with content : {}", server.getId(), content);
		try {
			m_serverService.updateServer(server);
		} catch (Exception e) {
			m_logger.error("Updating server :{} with content : {} failed!", server.getId(), content, e);
			throw new RestException("Updating server failed!", e);
		}

		return Response.ok().entity(server).build();

	}

	private Pair<Boolean, String> validateAndTrimServer(Server s) {
		if (StringUtils.isEmpty(s.getId())) {
			return new Pair<Boolean, String>(false, "Server id can not be null!");
		}
		
		s.setHost(s.getHost().trim());
		s.setIdc(s.getIdc().trim());

		return new Pair<Boolean, String>(true, null);

	}
}
