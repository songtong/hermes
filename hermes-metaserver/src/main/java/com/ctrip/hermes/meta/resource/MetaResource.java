package com.ctrip.hermes.meta.resource;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.server.RestException;
import com.ctrip.hermes.meta.service.MetaService;
import com.ctrip.hermes.meta.service.ServerMetaService;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static final Logger logger = Logger.getLogger(MetaResource.class);

	private MetaService metaService = PlexusComponentLocator.lookup(MetaService.class, ServerMetaService.ID);

	@GET
	public Response getMeta(@QueryParam("hashCode") long hashCode) {
		Meta meta = null;
		try {
			meta = metaService.getMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
			if (meta.hashCode() == hashCode) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			logger.warn(e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@GET
	@Path("refresh")
	public Response refreshMeta() {
		Meta meta = null;
		try {
			meta = metaService.getMeta(true);
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.warn(e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	public Response updateMeta(String content, @Context HttpServletRequest req) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Meta meta = null;
		try {
			meta = JSON.parseObject(content, Meta.class);
		} catch (Exception e) {
			logger.warn(e);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		try {
			boolean result = metaService.updateMeta(meta);
			if (result == false) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			logger.warn(e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(meta).build();
	}
}
