package com.ctrip.hermes.rest.resource;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Charsets;

@Path("/testpush")
public class TestPushTargetResource {

	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response push(byte[] b) {
		System.out.println(new String(b, Charsets.UTF_8));
		return Response.ok().build();
	}

}
