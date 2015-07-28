package com.ctrip.hermes.rest.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.google.common.io.ByteStreams;

@Path("/echo/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class EchoResource {

	private static final Logger logger = LoggerFactory.getLogger(EchoResource.class);

	private static class TopicTimeoutHandler implements TimeoutHandler {

		@Override
		public void handleTimeout(AsyncResponse asyncResponse) {
			asyncResponse.resume(Response.status(Status.REQUEST_TIMEOUT).entity("Timed out").build());
		}

	}

	private ExecutorService executor = Executors.newCachedThreadPool(HermesThreadFactory.create("MessageEcho", true));

	private void publishAsync(final String content, final AsyncResponse response) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					response.setTimeoutHandler(new TopicTimeoutHandler());
					response.resume(content);
				} catch (Exception e) {
					response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(e));
					response.cancel();
				}
			}

		});
	}

	private void publishAsync(final InputStream content, final AsyncResponse response) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					response.setTimeoutHandler(new TopicTimeoutHandler());
					response.resume(ByteStreams.toByteArray(content));
				} catch (Exception e) {
					response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(e));
					response.cancel();
				}
			}

		});
	}

	@Path("async")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public void echoAsync(@Context HttpHeaders headers, @Context HttpServletRequest request, InputStream content,
	      @Suspended final AsyncResponse response) throws IOException {
		logger.info("echo async: " + content);
		publishAsync(content, response);
	}

	@Path("async")
	@POST
	public void echoAsync(@Context HttpHeaders headers, @Context HttpServletRequest request, String content,
	      @Suspended final AsyncResponse response) throws IOException {
		logger.info("echo async: " + content);
		publishAsync(content, response);
	}

	@Path("sync")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public byte[] echoSync(InputStream content) throws IOException {
		byte[] byteArray = ByteStreams.toByteArray(content);
		logger.info("echo sync: " + new String(byteArray));
		return byteArray;
	}

	@Path("sync")
	@POST
	public String echoSync(String content) {
		logger.info("echo sync: " + content);
		return content;
	}
}
