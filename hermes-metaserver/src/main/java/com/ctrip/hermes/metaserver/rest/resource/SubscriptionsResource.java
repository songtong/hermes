package com.ctrip.hermes.metaserver.rest.resource;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.SubscriptionService;
import com.ctrip.hermes.metaservice.service.SubscriptionService.SubscriptionStatus;

@Path("/subscriptions/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SubscriptionsResource {

	private SubscriptionService subscriptionService = PlexusComponentLocator.lookup(SubscriptionService.class);

	@Path("/")
	@GET
	public List<SubscriptionView> getAll() {
		try {
			return subscriptionService.getSubscriptions();
		} catch (DalException e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("RUNNING")
	@GET
	public List<SubscriptionView> getRunning() {
		try {
			return subscriptionService.getSubscriptions(SubscriptionStatus.RUNNING);
		} catch (DalException e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("STOPPED")
	@GET
	public List<SubscriptionView> getStopped() {
		try {
			return subscriptionService.getSubscriptions(SubscriptionStatus.STOPPED);
		} catch (DalException e) {
			throw new InternalServerErrorException(e);
		}
	}
}
