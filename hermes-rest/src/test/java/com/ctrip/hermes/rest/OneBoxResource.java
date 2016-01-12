package com.ctrip.hermes.rest;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Charsets;

@Path("onebox")
public class OneBoxResource {

	@Path("hello")
	@GET
	public String hello() {
		return "Hello World";
	}

	@Path("push")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response push(@Context HttpHeaders headers, byte[] b) {
		System.out.println("Received: " + new String(b, Charsets.UTF_8));
		Map<String, String> receivedHeader = new HashMap<>();
		receivedHeader.put("X-Hermes-Topic", headers.getHeaderString("X-Hermes-Topic"));
		receivedHeader.put("X-Hermes-Ref-Key", headers.getHeaderString("X-Hermes-Ref-Key"));
		return Response.ok().build();
	}

	@Path("pushNotAvailable")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response pushServerError(byte[] b) {
		return Response.serverError().build();
	}

	@Path("pushTimout")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response pushTimeout(byte[] b) {
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
		}
		return Response.ok().build();
	}

	@Path("pushStandby")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response pushStandby(@Context HttpHeaders headers, byte[] b) {
		System.out.println("Received: " + new String(b, Charsets.UTF_8));
		Map<String, String> receivedHeader = new HashMap<>();
		receivedHeader.put("X-Hermes-Topic", headers.getHeaderString("X-Hermes-Topic"));
		receivedHeader.put("X-Hermes-Ref-Key", headers.getHeaderString("X-Hermes-Ref-Key"));
		return Response.ok().build();
	}
}
