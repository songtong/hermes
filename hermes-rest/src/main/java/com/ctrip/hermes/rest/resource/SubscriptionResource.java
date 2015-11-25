package com.ctrip.hermes.rest.resource;

import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;

@Path("/subscriptions/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SubscriptionResource {

	private SubscriptionRegisterService subscritionRegisterService = PlexusComponentLocator
	      .lookup(SubscriptionRegisterService.class);

	@Path("list")
	public Set<SubscriptionView> list() {
		return subscritionRegisterService.listSubscriptions();
	}

}
