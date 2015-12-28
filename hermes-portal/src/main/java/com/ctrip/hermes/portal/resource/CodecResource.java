package com.ctrip.hermes.portal.resource;

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.bo.CodecView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.metaservice.service.CodecService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/codecs/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CodecResource {

	private static final Logger logger = LoggerFactory.getLogger(CodecResource.class);

	private CodecService codecService = PlexusComponentLocator.lookup(CodecService.class);

	@GET
	@Path("{name}")
	public CodecView getCodec(@PathParam("name") String name) {
		logger.debug("get codec {}", name);
		Codec codec = codecService.getCodec(name);
		if (codec == null) {
			throw new RestException("Codec not found: " + name, Status.NOT_FOUND);
		}
		return new CodecView(codec);
	}

	@GET
	public Response getCodecs() {
		Map<String, Codec> codecs = codecService.getCodecs();
		return Response.status(Status.OK).entity(codecs.values()).build();
	}
}
