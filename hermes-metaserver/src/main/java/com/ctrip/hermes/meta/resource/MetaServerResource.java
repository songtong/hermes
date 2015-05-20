package com.ctrip.hermes.meta.resource;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.service.MetaService;
import com.ctrip.hermes.meta.service.ServerMetaService;

@Path("/metaserver/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaServerResource {

	private MetaService metaService = PlexusComponentLocator.lookup(MetaService.class, ServerMetaService.ID);

	@GET
	public List<String> getMetaServerIpPorts() {
		List<Server> servers = metaService.getServers();
		List<String> result = new ArrayList<>();
		for (Server server : servers) {
			result.add(String.format("%s:%s", server.getHost(), server.getPort()));
		}
		return result;
	}

}
