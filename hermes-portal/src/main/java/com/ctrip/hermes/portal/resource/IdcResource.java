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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.model.Idc;
import com.ctrip.hermes.metaservice.service.IdcService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/idcs/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class IdcResource {

	private static final Logger m_logger = LoggerFactory.getLogger(IdcResource.class);

	private IdcService m_idcService = PlexusComponentLocator.lookup(IdcService.class);

	@GET
	public Response getIdcs() {
		List<Idc> idcs = null;
		try {
			idcs = m_idcService.listIdcs();
		} catch (Exception e) {
			m_logger.error("Error in fetching idc list from db!", e);
			throw new RestException("Failed to get idc list!", Status.INTERNAL_SERVER_ERROR);
		}
		return Response.ok().entity(idcs).build();
	}

	@GET
	@Path("{idcId}")
	public Response getIdc(@PathParam("idcId") int idcId) {

		Idc idc = getValidIdc(idcId);

		return Response.ok().entity(idc).build();
	}

	@POST
	public Response addIdc(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("Query content is null!", Status.BAD_REQUEST);
		}
		Idc idc = null;
		try {
			idc = JSON.parseObject(content, Idc.class);
		} catch (Exception e) {
			m_logger.warn("Can not parse query content:{} to idc object!", content, e);
			throw new RestException("Can not parse query content to idc object!", Status.BAD_REQUEST);
		}

		m_logger.info("Creating new idc with payload:{}", content);
		try {
			m_idcService.addIdc(idc);
		} catch (Exception e) {
			m_logger.error("Error in inserting new idc into db!", e);
			throw new RestException("Failed to create new idc!", e);
		}

		return Response.status(Status.CREATED).entity(idc).build();
	}

	@DELETE
	@Path("{idcId}")
	public Response deleteIdc(@PathParam("idcId") int idcId) {
		m_logger.info("Deleting idc : {}", idcId);
		try {
			m_idcService.deleteIdc(idcId);
		} catch (Exception e) {
			m_logger.error("Deleting idc : {} failed!", idcId, e);
			throw new RestException("Deleting idc failed!", e);
		}
		return Response.ok().build();
	}

	@PUT
	public Response updateIdc(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("Payload can not bu null!", Status.BAD_REQUEST);
		}

		Idc idc = null;
		try {
			idc = JSON.parseObject(content, Idc.class);
		} catch (Exception e) {
			m_logger.warn("Can not parse query content:{} to idc object!", content, e);
			throw new RestException("Can not parse query content to idc object!", Status.BAD_REQUEST);
		}

		m_logger.info("Update idc : {} with content : {}", idc.getName(), content);
		try {
			m_idcService.updateIdc(idc);
		} catch (Exception e) {
			m_logger.error("Updating idc :{} with content : {} failed!", idc.getName(), content, e);
			throw new RestException("Updating idc failed!", e);
		}

		return Response.ok().entity(idc).build();
	}

	@PUT
	@Path("primary/{idc}")
	public Response switchPrimaryIdc(@PathParam("idc") int idc, @QueryParam("force") boolean force) {
		Idc targetIdc = getValidIdc(idc);
		if (!targetIdc.isEnabled()) {
			throw new RestException("Can not switch primary idc to a disabled idc!", Status.BAD_REQUEST);
		}
		m_logger.info("Switch primary idc to {}.", idc);
		try {
			if (force) {
				m_idcService.forceSwitchPrimary(idc);
			} else {
				m_idcService.switchPrimary(idc);
			}
		} catch (IllegalStateException e) {
			throw new RestException(e.getMessage(), Status.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			m_logger.error("Switching primary idc to {} failed!", idc, e);
			throw new RestException("Switching primary idc failed!", e);
		}
		return Response.ok().build();

	}

	@PUT
	@Path("enable/{idc}")
	public Response enableIdc(@PathParam("idc") int idc) {
		getValidIdc(idc);
		m_logger.info("Enable idc {}.", idc);
		try {
			m_idcService.enableIdc(idc);
		} catch (Exception e) {
			m_logger.error("Enable idc {} failed!", idc, e);
			throw new RestException("Enable idc failed!", e);
		}
		return Response.ok().build();

	}

	@PUT
	@Path("disable/{idc}")
	public Response disableIdc(@PathParam("idc") int idc) {
		Idc targetIdc = getValidIdc(idc);
		if (targetIdc.isPrimary()) {
			throw new RestException("Can not disabled primary idc!", Status.BAD_REQUEST);
		}
		m_logger.info("Disable idc {}.", idc);
		try {
			m_idcService.disableIdc(idc);
		} catch (Exception e) {
			m_logger.error("Disable idc {} failed!", idc, e);
			throw new RestException("Disable idc failed!", e);
		}
		return Response.ok().build();

	}

	private Idc getValidIdc(int idc) {
		Idc targetIdc = null;
		try {
			targetIdc = m_idcService.findIdc(idc);
		} catch (Exception e) {
			m_logger.error("Error in getting idc : {} from db!", idc, e);
			throw new RestException(String.format("Can not get idc : %s!", idc), Status.INTERNAL_SERVER_ERROR);
		}
		if (targetIdc == null) {
			throw new RestException(String.format("Can not find idc : %s!", idc), Status.NOT_FOUND);
		}

		return targetIdc;
	}

}
