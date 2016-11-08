package com.ctrip.hermes.portal.resource;

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.model.ZookeeperEnsemble;
import com.ctrip.hermes.metaservice.service.ServerService;
import com.ctrip.hermes.metaservice.service.ZookeeperEnsembleService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.zookeeperMigration.DefaultZookeeperMigrationService.CheckResult;
import com.ctrip.hermes.portal.service.zookeeperMigration.ZookeeperMigrationService;

@Path("/zookeepers/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ZookeeperResource {
	private static final Logger m_logger = LoggerFactory.getLogger(ZookeeperResource.class);

	private ZookeeperMigrationService m_zookeeperMigrationService = PlexusComponentLocator
	      .lookup(ZookeeperMigrationService.class);

	private ZookeeperEnsembleService m_zookeeperEnsembleService = PlexusComponentLocator
	      .lookup(ZookeeperEnsembleService.class);

	private ServerService m_serverService = PlexusComponentLocator.lookup(ServerService.class);

	private ZKClient m_zkClient = PlexusComponentLocator.lookup(ZKClient.class);

	@GET
	public Response getZookeeperEnsembles() {
		try {
			return Response.ok().entity(m_zookeeperEnsembleService.listEnsembleModels()).build();
		} catch (DalException e) {
			m_logger.error("Get zookeeperEnsembles Failed!", e);
			throw new RestException("Get zookeeperEnsembles Failed!", e);
		}
	}

	@POST
	public Response addZookeeperEnsemble(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("Query content is null!", Status.BAD_REQUEST);
		}
		ZookeeperEnsemble zookeeperEnsemble = null;
		try {
			zookeeperEnsemble = JSON.parseObject(content, ZookeeperEnsemble.class);
		} catch (Exception e) {
			m_logger.warn("Can not parse query content:{} to zookeeperEnsemble object!", content, e);
			throw new RestException("Can not parse query content to zookeeperEnsemble object!", Status.BAD_REQUEST);
		}

		m_logger.info("Creating new zookeeperEnsemble with payload:{}", content);
		try {
			m_zookeeperEnsembleService.addZookeeperEnsemble(zookeeperEnsemble);
		} catch (Exception e) {
			m_logger.error("Error in inserting new zookeeperEnsemble into db!", e);
			throw new RestException("Failed to create new zookeeperEnsemble!", e);
		}

		return Response.status(Status.CREATED).entity(zookeeperEnsemble).build();
	}

	@DELETE
	@Path("{zookeeperEnsembleId}")
	public Response deleteZookeeperEnsemble(@PathParam("zookeeperEnsembleId") int zookeeperEnsembleId) {
		m_logger.info("Deleting zookeeperEnsemble : {}", zookeeperEnsembleId);
		try {
			m_zookeeperEnsembleService.deleteZookeeperEnsemble(zookeeperEnsembleId);
		} catch (Exception e) {
			m_logger.error("Deleting zookeeperEnsemble : {} failed!", zookeeperEnsembleId, e);
			throw new RestException("Deleting zookeeperEnsemble failed!", e);
		}
		return Response.ok().build();
	}

	@PUT
	public Response updateZookeeperEnsemble(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("Payload can not bu null!", Status.BAD_REQUEST);
		}

		ZookeeperEnsemble zookeeperEnsemble = null;
		try {
			zookeeperEnsemble = JSON.parseObject(content, ZookeeperEnsemble.class);
		} catch (Exception e) {
			m_logger.warn("Can not parse query content:{} to zookeeperEnsemble object!", content, e);
			throw new RestException("Can not parse query content to zookeeperEnsemble object!", Status.BAD_REQUEST);
		}

		m_logger.info("Update zookeeperEnsemble : {} with content : {}", zookeeperEnsemble.getName(), content);
		try {
			m_zookeeperEnsembleService.updateZookeeperEnsemble(zookeeperEnsemble);
		} catch (Exception e) {
			m_logger.error("Updating zookeeperEnsemble :{} with content : {} failed!", zookeeperEnsemble.getName(),
			      content, e);
			throw new RestException("Updating zookeeperEnsemble failed!", e);
		}

		return Response.ok().entity(zookeeperEnsemble).build();
	}

	@PUT
	@Path("primary")
	public Response switchPrimaryZookeeperEnsemble(@QueryParam("zookeeperEnsembleId") int zookeeperEnsembleId) {
		m_logger.info("switch primary zookeeperEnsemble to {}", zookeeperEnsembleId);
		try {
			m_zookeeperEnsembleService.switchPrimaryZookeeperEnsemble(zookeeperEnsembleId);
		} catch (Exception e) {
			m_logger.error("Switching primary to zookeeperEnsemble : {} failed!", zookeeperEnsembleId, e);
			throw new RestException("Switching primary to zookeeperEnsemble failed!", e);
		}
		m_logger.info("Switching primary to zookeeperEnsemble(id={}) success!", zookeeperEnsembleId);
		return Response.ok().build();
	}

	@POST
	@Path("initialize")
	public Response initializeZookeeperEnsemble(@QueryParam("zookeeperEnsembleId") int zookeeperEnsembleId) {
		m_logger.info("Start initialize zookeeperEnsemble(id={}).", zookeeperEnsembleId);

		ZookeeperEnsemble zookeeperEnsemble;
		try {
			zookeeperEnsemble = m_zookeeperEnsembleService.findZookeeperEnsembleModelById(zookeeperEnsembleId);
		} catch (Exception e) {
			throw new RestException(e.getMessage(), e);
		}

		if (zookeeperEnsemble == null) {
			throw new RestException(String.format("Can not find zookeeperEnsemble(id=%s)", zookeeperEnsembleId),
			      Status.BAD_REQUEST);
		}

		try {
			m_zookeeperMigrationService.initializeZkFromBaseMeta(zookeeperEnsembleId);
		} catch (Exception e) {
			m_logger.error("Initialize zookeeperEnsemble(id={}) failed!", zookeeperEnsembleId, e);
			throw new RestException(e.getMessage(), e);
		}

		m_logger.info("Initialize zookeeperEnsemble(id={}) success.", zookeeperEnsembleId);
		return Response.ok().build();
	}

	@POST
	@Path("leaseAssigning/stop")
	public Response stopLeaseAssigning(@QueryParam("serverId") String serverId) {
		m_logger.info("Stop lease assigning for server(id={}).", serverId);

		Server server;
		try {
			server = m_serverService.findServerByName(serverId);
		} catch (DalException e) {
			m_logger.error("Find server by name: {} failed!", serverId, e);
			throw new RestException(String.format("Find server by id: %s failed!", serverId));
		}

		if (server == null) {
			m_logger.info("Server({}) not exits", serverId);
			throw new RestException(String.format("Server(%s) not exits", serverId));
		}

		try {
			CheckResult checkResult = m_zookeeperMigrationService.stopLeaseAssigningAndCheckStatus(server);
			return Response.ok().entity(checkResult).build();
		} catch (Exception e) {
			throw new RuntimeException("Stop lease assigning failed!", e);
		}
	}

	@POST
	@Path("leaseAssigning/stopAll")
	public Response stopLeaseAssigningForAllMetaServers() {
		m_logger.info("Stop lease assigning for all metaservers");

		try {
			Map<String, CheckResult> checkResults = m_zookeeperMigrationService
			      .stopLeaseAssigningAndCheckStatusForAllMetaServers();
			return Response.ok().entity(checkResults).build();
		} catch (Exception e) {
			throw new RestException("Stop lease assigning for all metaServers failed!", e);
		}
	}

	@POST
	@Path("pauseAndSwitch")
	public Response pauseAndSwitch(@QueryParam("serverId") String serverId) {
		m_logger.info("Pause and switch zk for server(id={}).", serverId);

		Server server;
		try {
			server = m_serverService.findServerByName(serverId);
		} catch (DalException e) {
			m_logger.error("Find server by name: {} failed!", serverId, e);
			throw new RestException(String.format("Find server by id: %s failed!", serverId));
		}

		if (server == null) {
			m_logger.info("Server({}) not exits", serverId);
			throw new RestException(String.format("Server(%s) not exits", serverId));
		}

		try {
			CheckResult checkResult = m_zookeeperMigrationService.pauseAndSwitchMetaServerZkEnsembleAndCheckStatus(server);
			return Response.ok().entity(checkResult).build();
		} catch (Exception e) {
			throw new RuntimeException("Pause and switch zk failed!", e);
		}
	}

	@POST
	@Path("pauseAndSwitch/all")
	public Response pauseAndSwitchAll() {
		m_logger.info("Pause and switch zk for all metaservers");

		try {
			Map<String, CheckResult> checkResults = m_zookeeperMigrationService
			      .pauseAndSwitchAllMetaServersZkEnsembleAndCheckStatus();
			return Response.ok().entity(checkResults).build();
		} catch (Exception e) {
			throw new RestException("Pause and switch zk for all metaServers failed!", e);
		}
	}

	@POST
	@Path("resume")
	public Response resume(@QueryParam("serverId") String serverId) {
		m_logger.info("Resume zk for server(id={}).", serverId);

		Server server;
		try {
			server = m_serverService.findServerByName(serverId);
		} catch (DalException e) {
			m_logger.error("Find server by name: {} failed!", serverId, e);
			throw new RestException(String.format("Find server by id: %s failed!", serverId));
		}

		if (server == null) {
			m_logger.info("Server({}) not exits", serverId);
			throw new RestException(String.format("Server(%s) not exits", serverId));
		}

		try {
			CheckResult checkResult = m_zookeeperMigrationService.resumeMetaServerAndCheckStatus(server);
			return Response.ok().entity(checkResult).build();
		} catch (Exception e) {
			throw new RuntimeException("Resume zk failed!", e);
		}
	}

	@POST
	@Path("resume/all")
	public Response resumeAll() {
		m_logger.info("Resume zk for all metaservers");

		try {
			Map<String, CheckResult> checkResults = m_zookeeperMigrationService.resumeAllMetaServersAndCheckStatus();
			return Response.ok().entity(checkResults).build();
		} catch (Exception e) {
			throw new RestException("Resume zk for all metaServers failed!", e);
		}
	}

	@POST
	@Path("leaseAssigning/start")
	public Response startLeaseAssigning(@QueryParam("serverId") String serverId) {
		m_logger.info("Start lease assigning for server(id={}).", serverId);

		Server server;
		try {
			server = m_serverService.findServerByName(serverId);
		} catch (DalException e) {
			m_logger.error("Find server by name: {} failed!", serverId, e);
			throw new RestException(String.format("Find server by id: %s failed!", serverId));
		}

		if (server == null) {
			m_logger.info("Server({}) not exits", serverId);
			throw new RestException(String.format("Server(%s) not exits", serverId));
		}

		try {
			CheckResult checkResult = m_zookeeperMigrationService.startLeaseAssigningAndCheckStatus(server);
			return Response.ok().entity(checkResult).build();
		} catch (Exception e) {
			throw new RuntimeException("Start lease assigning failed!", e);
		}
	}

	@POST
	@Path("leaseAssigning/startAll")
	public Response startLeaseAssigningForAllMetaServers() {
		m_logger.info("Start lease assigning for all metaservers");

		try {
			Map<String, CheckResult> checkResults = m_zookeeperMigrationService
			      .startLeaseAssigningAndCheckStatusForAllMetaServers();
			return Response.ok().entity(checkResults).build();
		} catch (Exception e) {
			throw new RestException("Start lease assigning for all metaServers failed!", e);
		}
	}

	@GET
	@Path("runningBrokers")
	public Response getRunningBrokers() {
		try {
			Map<String, CheckResult> checkResults = m_zookeeperMigrationService.getRunningBrokers();
			return Response.ok().entity(checkResults).build();
		} catch (Exception e) {
			throw new RestException(e.getMessage());
		}
	}

	@POST
	@Path("pauseAndSwitch/self")
	public Response pauseAndwitchZK() {
		try {
			boolean switched = m_zkClient.pauseAndSwitchPrimaryEnsemble();
			return Response.status(Status.OK).entity(switched).build();
		} catch (Exception e) {
			throw new RestException("Switch failed!", e);
		}
	}

	@POST
	@Path("resume/self")
	public Response resumeZK() {
		m_zkClient.resume();
		return Response.status(Status.OK).entity(Boolean.TRUE).build();
	}

}
