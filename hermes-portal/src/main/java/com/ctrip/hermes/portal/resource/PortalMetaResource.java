package com.ctrip.hermes.portal.resource;

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
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.service.MetaRefactor;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.meta.DefaultPortalMetaService;
import com.ctrip.hermes.portal.util.MetaDiffer.MetaDiff;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PortalMetaResource {

	private static final Logger logger = LoggerFactory.getLogger(PortalMetaResource.class);

	private DefaultPortalMetaService metaService = PlexusComponentLocator.lookup(DefaultPortalMetaService.class);

	private MetaRefactor metaRefactor = PlexusComponentLocator.lookup(MetaRefactor.class);

	@GET
	public Response getMeta() {
		Meta meta = null;
		try {
			meta = metaService.getMetaEntity();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.warn("get meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@GET
	@Path("refresh")
	public Response refreshMeta() {
		Meta meta = null;
		try {
			meta = metaService.refreshMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.warn("refresh meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@GET
	@Path("preview")
	public Response previewNewMeta() {
		Meta meta = null;
		try {
			meta = metaService.previewNewMeta();
		} catch (DalException e) {
			logger.warn("preview meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	@Path("build")
	public Response buildNewMeta() {
		Meta meta = null;
		try {
			meta = metaService.buildNewMeta();
		} catch (DalException e) {
			logger.warn("build meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	@Path("refactor")
	public Response refactorMeta() {
		try {
			metaRefactor.refactor();
		} catch (Exception e) {
			logger.warn("refactor meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@GET
	@Path("diff")
	public Response getMetaDiff() {
		MetaDiff metaDiff = null;
		try {
			metaDiff = metaService.getMetaDiff();
		} catch (Exception e) {
			logger.warn("Failed to get meta diff!", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.ok().entity(metaDiff).build();
	}
}
