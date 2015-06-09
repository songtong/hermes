package com.ctrip.hermes.metaserver.rest.resource;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.rest.commons.RestException;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static final Logger logger = LoggerFactory.getLogger(MetaResource.class);

	private MetaHolder m_metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

	@GET
	public Response getMeta(@QueryParam("version") @DefaultValue("0") int version,
	      @QueryParam("hashCode") @DefaultValue("0") long hashCode) {
		logger.debug("get meta, version {}", version);
		Meta meta = null;
		try {
			meta = m_metaHolder.getMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
			if (version > 0 && meta.getVersion().equals(version)) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
			if (hashCode > 0 && meta.hashCode() == hashCode) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			logger.warn("get meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

}
