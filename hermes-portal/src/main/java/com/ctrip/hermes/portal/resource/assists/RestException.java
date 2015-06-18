package com.ctrip.hermes.portal.resource.assists;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.plexus.util.ExceptionUtils;

import com.alibaba.fastjson.JSON;

public class RestException extends WebApplicationException {
	private static final long serialVersionUID = -5416250813243019949L;

	public RestException(Exception e) {
		this(e, Response.Status.INTERNAL_SERVER_ERROR);
	}

	public RestException(Exception e, Response.Status status) {
		this(e.getMessage(), status);
	}

	public RestException(String content) {
		this(content, Response.Status.INTERNAL_SERVER_ERROR);
	}

	public RestException(String content, Exception e) {
		this(content + "\n" + ExceptionUtils.getStackTrace(e));
	}

	public RestException(String content, Response.Status status) {
		super(Response.status(status).entity(JSON.toJSONString(content)).type(MediaType.APPLICATION_JSON).build());
	}
}