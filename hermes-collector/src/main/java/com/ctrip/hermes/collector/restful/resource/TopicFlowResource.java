package com.ctrip.hermes.collector.restful.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.stereotype.Component;

@Component
@Path("/flow")
public class TopicFlowResource {
	@GET
	public Response getTopicFlow() {
		return Response.status(Status.OK).entity("test").build();
	}

}
