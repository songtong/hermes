package com.ctrip.hermes.rest.resource;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.ProducerService;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicsResource {

	private static final Logger logger = LoggerFactory.getLogger(TopicsResource.class);

	private ProducerService producerService = PlexusComponentLocator.lookup(ProducerService.class);

	private ExecutorService executor = Executors.newCachedThreadPool();

	private void publishAsync(final String topic, final Map<String, String> params, final InputStream content,
	      final AsyncResponse response) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					Future<SendResult> sendResult = producerService.send(topic, params, content);
					response.resume(sendResult.get());
					// TODO timeout
				} catch (Exception e) {
					response.resume(e);
					response.cancel();
				}
			}

		});
	}

	@Path("{topicName}")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public void publishBinary(@PathParam("topicName") String topicName, @Context HttpHeaders headers,
	      InputStream content, @Suspended final AsyncResponse response) {
		if (!producerService.topicExist(topicName)) {
			throw new BadRequestException(String.format("Topic {0} does not exist", topicName));
		}

		if (logger.isTraceEnabled()) {
			logger.trace("{} {} {}", topicName, headers.getRequestHeaders().toString(), content);
		}

		MultivaluedMap<String, String> requestHeaders = headers.getRequestHeaders();
		Map<String, String> params = new HashMap<>();
		params.put("partitionKey", requestHeaders.getFirst("partitionKey"));
		publishAsync(topicName, params, content, response);
	}

//	@Path("{topicName}")
//	@POST
//	@Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, MediaType.TEXT_HTML })
//	public void publishString(@PathParam("topicName") String topicName, @Context HttpHeaders headers, String content,
//	      @Suspended final AsyncResponse response) {
//		if (!producerService.topicExist(topicName)) {
//			throw new BadRequestException(String.format("Topic {0} does not exist", topicName));
//		}
//
//		if (logger.isTraceEnabled()) {
//			logger.trace("{} {} {}", topicName, headers.getRequestHeaders().toString(), content);
//		}
//
//		MultivaluedMap<String, String> requestHeaders = headers.getRequestHeaders();
//		Map<String, String> params = new HashMap<>();
//		params.put("partitionKey", requestHeaders.getFirst("partitionKey"));
//		publishAsync(topicName, params, content, response);
//	}
}
