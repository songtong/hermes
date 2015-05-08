package com.ctrip.hermes.meta.resource;

import java.util.Arrays;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/metaserver/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaServerResource {

	@GET
	public List<String> getMetaServerIpPorts() {
		// TODO
		return Arrays.asList("127.0.0.1:1248");
	}

}
