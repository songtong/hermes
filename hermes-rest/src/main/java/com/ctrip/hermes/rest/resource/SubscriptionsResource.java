package com.ctrip.hermes.rest.resource;

import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.SubscribeRegistry;
import com.ctrip.hermes.rest.service.Subscription;

@Path("/subscriptions/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SubscriptionsResource {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionsResource.class);

	private SubscribeRegistry subscribeRegistry = PlexusComponentLocator.lookup(SubscribeRegistry.class);

	@Path("")
	@GET
	public Map<String, List<Subscription>> getAll() {
		return subscribeRegistry.getSubscriptions();
	}

	@Path("{topicName}")
	@GET
	public List<Subscription> getByTopic(@PathParam("topicName") String topicName) {
		return subscribeRegistry.getSubscriptions(topicName);
	}

	@Path("{topicName}/sub")
	@POST
	public Response subscribe(@PathParam("topicName") String topicName, String content) {
		logger.debug("subscribe {} {}", topicName, content);

		Subscription subscription = null;
		try {
			subscription = JSON.parseObject(content, Subscription.class);
		} catch (Exception e) {
			throw new BadRequestException("Parse subscription failed", e);
		}

		try {
			subscribeRegistry.register(subscription);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}

		return Response.status(Status.CREATED).build();
	}

	@Path("{topicName}/unsub")
	@POST
	public Response unsubscribe(@PathParam("topicName") String topicName, String content) {
		logger.debug("unsubscribe {} {}", topicName, content);

		Subscription subscription = null;
		try {
			subscription = JSON.parseObject(content, Subscription.class);
		} catch (Exception e) {
			throw new BadRequestException("Parse subscription failed", e);
		}

		try {
			subscribeRegistry.unregister(subscription);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}

		return Response.status(Status.OK).build();
	}
}
