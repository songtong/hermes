package com.ctrip.hermes.rest.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
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

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.ProducerService;

@Path("/producer/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ProducerResource {

	private ProducerService producerService = PlexusComponentLocator.lookup(ProducerService.class);

	private ExecutorService executor = Executors.newCachedThreadPool();

	@Path("publish/{topicName}")
	@POST
	public void publish(@PathParam("topicName") String topicName, @Context HttpHeaders headers, String content,
	      @Suspended final AsyncResponse response) {
		if (!producerService.topicExist(topicName)) {
			throw new BadRequestException(String.format("Topic {0} does not exist", topicName));
		}

		MultivaluedMap<String, String> requestHeaders = headers.getRequestHeaders();
		Map<String, String> params = new HashMap<>();
		params.put("partitionKey", requestHeaders.getFirst("partitionKey"));
		publishAsync(topicName, params, content, response);
	}

	private void publishAsync(final String topic, final Map<String, String> params, final String content,
	      final AsyncResponse response) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					Future<SendResult> sendResult = producerService.send(topic, params, content);
					response.resume(sendResult.get());
				} catch (Exception e) {
					response.resume(e);
					response.cancel();
				}
			}

		});
	}
}
