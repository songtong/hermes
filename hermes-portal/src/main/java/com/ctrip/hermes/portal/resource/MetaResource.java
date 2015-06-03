package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

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

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.portal.server.RestException;
import com.ctrip.hermes.portal.service.MetaServiceWrapper;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static final Logger logger = LoggerFactory.getLogger(MetaResource.class);

	private MetaServiceWrapper metaService = PlexusComponentLocator.lookup(MetaServiceWrapper.class);

	@GET
	@Path("codecs")
	public Response getCodecs() {
		List<Codec> codecs = new ArrayList<Codec>(metaService.getCodecs().values());
		Collections.sort(codecs, new Comparator<Codec>() {
			@Override
			public int compare(Codec o1, Codec o2) {
				return o1.getType().compareTo(o2.getType());
			}
		});
		return Response.status(Status.OK).entity(codecs).build();
	}

	@GET
	@Path("storages")
	public Response getStorages() {
		List<Storage> storages = new ArrayList<Storage>(metaService.getStorages().values());
		Collections.sort(storages, new Comparator<Storage>() {
			@Override
			public int compare(Storage o1, Storage o2) {
				return o1.getType().compareTo(o2.getType());
			}
		});
		return Response.status(Status.OK).entity(storages).build();
	}

	@GET
	@Path("endpoints")
	public Response getEndpoints() {
		List<Endpoint> endpoints = new ArrayList<Endpoint>(metaService.getEndpoints().values());
		Collections.sort(endpoints, new Comparator<Endpoint>() {
			@Override
			public int compare(Endpoint o1, Endpoint o2) {
				return o1.getType().compareTo(o2.getType());
			}
		});
		return Response.status(Status.OK).entity(endpoints).build();
	}

	@GET
	public Response getMeta(@QueryParam("hashCode") long hashCode) {
		logger.debug("get meta, hashCode {}", hashCode);
		Meta meta = null;
		try {
			meta = metaService.findLatestMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
			if (meta.hashCode() == hashCode) {
				return Response.status(Status.NOT_MODIFIED).build();
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
			meta = metaService.findLatestMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.warn("refresh meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	public Response updateMeta(String content, @Context HttpServletRequest req) {
		logger.debug("update meta, content {}", content);
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Meta meta = null;
		try {
			meta = JSON.parseObject(content, Meta.class);
		} catch (Exception e) {
			logger.warn("parse meta failed with content:{}", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		try {
			boolean result = metaService.updateMeta(meta);
			if (result == false) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			logger.warn("update meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(meta).build();
	}
}
