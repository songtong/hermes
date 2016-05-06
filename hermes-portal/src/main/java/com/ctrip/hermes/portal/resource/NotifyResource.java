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
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.storage.NoticeStorage;

@Path("/notify")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class NotifyResource {
	private NoticeStorage m_storage = PlexusComponentLocator.lookup(NoticeStorage.class);

	@GET
	@Path("/sms")
	public Response getLatestMonitorEvents(@QueryParam("queryOnly") @DefaultValue("true") boolean queryOnly) {
		List<HermesNotice> notifications = m_storage.findSmsNotices(queryOnly);
		return Response.status(Status.OK).entity(notifications).build();
	}
}
