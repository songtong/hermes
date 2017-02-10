package com.ctrip.hermes.rest.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
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

import com.ctrip.fx.ubt.bridge.http.ServerEventReceiver;
import com.ctrip.fx.ubt.bridge.http.impl.DefaultServerEventReceiver;
import com.ctrip.fx.ubt.bridge.http.model.HermesMessage;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

@Path("/spaceport/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SpaceportResource {

	private static final Logger logger = LoggerFactory.getLogger(SpaceportResource.class);

	private static final FileBizLogger bizLogger = PlexusComponentLocator.lookup(FileBizLogger.class);

	private ServerEventReceiver<HermesMessage> m_receiver = new DefaultServerEventReceiver();

	private ClientEnvironment env = PlexusComponentLocator.lookup(ClientEnvironment.class);

	private class TopicTimeoutHandler implements TimeoutHandler {

		@Override
		public void handleTimeout(AsyncResponse asyncResponse) {
			asyncResponse.resume(Response.status(Status.REQUEST_TIMEOUT).build());
		}

	}

	@POST
	@Path("q")
	public void ubt(@Context HttpServletRequest request, @Context HttpHeaders headers, InputStream content,
	      @Suspended final AsyncResponse response) {
		try {
			Map<String, String> headerMap = new HashMap<>();
			for (Entry<String, List<String>> header : headers.getRequestHeaders().entrySet()) {
				headerMap.put(header.getKey(), header.getValue().get(0));
			}
			List<? extends HermesMessage> messages = new ArrayList<>();
			try {
				messages = m_receiver.receive(headerMap, content);
			} catch (IOException e) {
				logger.error("Failed to call ubt api!", e);
				response.resume(e);
				return;
			}

			List<Future<SendResult>> futures = new ArrayList<>(messages.size());

			for (HermesMessage hermesMessage : messages) {
				BizEvent receiveEvent = new BizEvent("Ubt.received");
				receiveEvent.addData("topic",
				      PlexusComponentLocator.lookup(MetaService.class).findTopicByName(hermesMessage.getTopic()).getId());
				receiveEvent.addData("refKey", hermesMessage.getRefKey());
				receiveEvent.addData("remoteHost", request.getRemoteHost());
				bizLogger.log(receiveEvent);

				MessageHolder message = Producer.getInstance()
				      .message(hermesMessage.getTopic(), hermesMessage.getPartitionKey(), hermesMessage.getPayload())
				      .withRefKey(hermesMessage.getRefKey()).setCallback(new CompletionCallback<SendResult>() {

					      @Override
					      public void onSuccess(SendResult result) {

					      }

					      @Override
					      public void onFailure(Throwable t) {
						      if (t instanceof MessageSendException) {
							      MessageSendException e = (MessageSendException) t;
							      logger.warn("Failed to send UBT message to topic: {}! Reason: {}.", e.getRawMessage()
							            .getTopic(), e.getCause());
						      } else {
							      logger.warn("Send UBT message failed!", t);
						      }
					      }
				      });

				for (Entry<String, String> property : hermesMessage.getProperties().entrySet()) {
					message.addProperty(property.getKey(), property.getValue());
				}

				futures.add(message.send());

			}

			response.setTimeout(
			      Integer.valueOf(env.getGlobalConfig().getProperty("gateway.topic.publish.timeout", "3000")),
			      TimeUnit.MILLISECONDS);

			response.setTimeoutHandler(new TopicTimeoutHandler());
			for (Future<SendResult> future : futures) {
				future.get();
			}
			response.resume(Response.status(Status.OK));
		} catch (Exception e) {
			response.resume(Response.status(Status.INTERNAL_SERVER_ERROR).entity(e));
			response.cancel();
		}
	}
}
