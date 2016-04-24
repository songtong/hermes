package com.ctrip.hermes.portal.resource;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.notify.HermesNotification;
import com.ctrip.hermes.metaservice.service.notify.storage.NotificationStorage;

@Path("/notify")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class NotifyResource {
	private NotificationStorage m_storage = PlexusComponentLocator.lookup(NotificationStorage.class);

	@GET
	@Path("/sms")
	public Response getLatestMonitorEvents(@QueryParam("queryOnly") @DefaultValue("true") boolean queryOnly) {
		List<HermesNotification> notifications = m_storage.findSmsNotifications(queryOnly);
		return Response.status(Status.OK).entity(notifications).build();
	}
}
