package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DELETE;
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
import com.ctrip.hermes.meta.entity.Subscription;
import com.ctrip.hermes.portal.service.SubscriptionService;

@Path("/subscriptions/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SubscriptionsResource {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionsResource.class);

	private SubscriptionService subscriptionService = PlexusComponentLocator.lookup(SubscriptionService.class);

	@Path("")
	@GET
	public List<Subscription> getAll() {
		List<Subscription> l = new ArrayList<Subscription>();
		for (Entry<String, Subscription> entry : subscriptionService.getSubscriptions().entrySet()) {
			l.add(entry.getValue());
		}
		Collections.sort(l, new Comparator<Subscription>() {
			@Override
			public int compare(Subscription o1, Subscription o2) {
				int ret = o1.getTopic().compareTo(o2.getTopic());
				ret = ret == 0 ? o1.getGroup().compareTo(o2.getGroup()) : ret;
				return ret;
			}
		});
		return l;
	}

	@Path("/")
	@POST
	public Response subscribe(String content) {
		logger.debug("subscribe {}", content);

		Subscription subscription = null;
		try {
			subscription = JSON.parseObject(content, Subscription.class);
		} catch (Exception e) {
			throw new BadRequestException("Parse subscription failed", e);
		}

		try {
			subscriptionService.create(subscription);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}

		return Response.status(Status.CREATED).build();
	}

	@Path("{id}")
	@DELETE
	public Response unsubscribe(@PathParam("id") String id) {
		logger.debug("unsubscribe {} {}", id);

		try {
			subscriptionService.remove(id);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}

		return Response.status(Status.OK).build();
	}
}
