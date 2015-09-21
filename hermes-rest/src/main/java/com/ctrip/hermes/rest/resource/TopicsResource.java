package com.ctrip.hermes.rest.resource;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.ctrip.hermes.Hermes.Env;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.BizLogger;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.ProducerSendService;
import com.ctrip.hermes.rest.status.TopicsResourceStatusMonitor;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicsResource {

	private class TopicTimeoutHandler implements TimeoutHandler {

		private String topic;

		public TopicTimeoutHandler(String topic) {
			this.topic = topic;
		}

		@Override
		public void handleTimeout(AsyncResponse asyncResponse) {
			TopicsResourceStatusMonitor.INSTANCE.updateTimeoutMeter(topic);
			asyncResponse.resume(Response.status(Status.REQUEST_TIMEOUT).entity("Timed out").build());
		}

	}

	private static final Logger logger = LoggerFactory.getLogger(TopicsResource.class);

	private static final BizLogger bizLogger = PlexusComponentLocator.lookup(BizLogger.class);

	public static final String PARTITION_KEY = "X-Hermes-Partition-Key";

	public static final String PRIORITY = "X-Hermes-Priority-Message";

	public static final String REF_KEY = "X-Hermes-Ref-Key";

	public static final String PROPERTIES = "X-Hermes-Message-Property";

	private ProducerSendService producerService = PlexusComponentLocator.lookup(ProducerSendService.class);

	private ClientEnvironment env = PlexusComponentLocator.lookup(ClientEnvironment.class);

	private ThreadPoolExecutor executor;

	public TopicsResource() {
		executor = (ThreadPoolExecutor) Executors
		      .newCachedThreadPool(HermesThreadFactory.create("message-publish", true));
		TopicsResourceStatusMonitor.INSTANCE.monitorExecutor(executor);
	}

	private Map<String, String> extractHeaderParams(HttpHeaders headers) {
		MultivaluedMap<String, String> requestHeaders = headers.getRequestHeaders();
		Map<String, String> params = new HashMap<>();
		if (requestHeaders.containsKey(PARTITION_KEY)) {
			params.put("partitionKey", requestHeaders.getFirst(PARTITION_KEY));
		}
		if (requestHeaders.containsKey(PRIORITY)) {
			params.put("priority", requestHeaders.getFirst(PRIORITY));
		}
		if (requestHeaders.containsKey(REF_KEY)) {
			params.put("refKey", requestHeaders.getFirst(REF_KEY));
		}
		if (requestHeaders.containsKey(PROPERTIES)) {
			params.put("properties", requestHeaders.getFirst(PROPERTIES));
		}
		return params;
	}

	private void publishAsync(final String topic, final Map<String, String> params, final InputStream content,
	      final AsyncResponse response) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				final Timer.Context contextGlobal = TopicsResourceStatusMonitor.INSTANCE.getSendTimerGlobal().time();
				final Timer.Context context = TopicsResourceStatusMonitor.INSTANCE.getSendTimer(topic).time();
				try {
					if (env.getEnv() == Env.PROD) {
						response.setTimeout(
						      Integer.valueOf(env.getGlobalConfig().getProperty("gateway.topic.publish.timeout", "1000")),
						      TimeUnit.MILLISECONDS);
					}
					response.setTimeoutHandler(new TopicTimeoutHandler(topic));
					Future<SendResult> sendResult = producerService.send(topic, params, content);
					response.resume(sendResult.get());
				} catch (Exception e) {
					response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(e));
					response.cancel();
				} finally {
					context.stop();
					contextGlobal.stop();
				}
			}

		});
	}

	@Path("{topicName}")
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public void publishBinary(@PathParam("topicName") String topicName, @Context HttpHeaders headers,
	      @Context HttpServletRequest request, InputStream content, @Suspended final AsyncResponse response) {
		if (!producerService.topicExist(topicName)) {
			throw new NotFoundException(String.format("Topic %s does not exist", topicName));
		}

		if (logger.isTraceEnabled()) {
			logger.trace("{} {} {}", topicName, headers.getRequestHeaders().toString(), content);
		}

		Map<String, String> params = extractHeaderParams(headers);

		BizEvent receiveEvent = new BizEvent("Rest.received");
		receiveEvent.addData("topic", topicName);
		receiveEvent.addData("refKey", params.get("refKey"));
		receiveEvent.addData("remoteHost", request.getRemoteHost());
		bizLogger.log(receiveEvent);

		TopicsResourceStatusMonitor.INSTANCE.updateRequestMeter(topicName);
		TopicsResourceStatusMonitor.INSTANCE.updateRequestSizeHistogram(topicName, request.getContentLength());

		publishAsync(topicName, params, content, response);
	}

}
