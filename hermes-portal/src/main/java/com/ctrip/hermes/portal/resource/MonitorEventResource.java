package com.ctrip.hermes.portal.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;
import com.ctrip.hermes.metaservice.monitor.dao.MonitorEventStorage;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;

@Path("/monitor/event/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MonitorEventResource {
	private MonitorEventStorage m_eventStorage = PlexusComponentLocator.lookup(MonitorEventStorage.class);

	@GET
	@Path("latest")
	public Response getLatestMonitorEvents(@QueryParam("forNotify") boolean forNotify) {
		List<MonitorEvent> events = m_eventStorage.fetchUnnotifiedMonitorEvent(forNotify);
		return Response.status(Status.OK).entity(classifyEvents(events)).build();
	}

	private Map<String, Integer> classifyEvents(List<MonitorEvent> events) {
		Map<String, Integer> m = new HashMap<>();
		if (events != null) {
			for (MonitorEventType e : MonitorEventType.values()) {
				m.put(e.getDisplayName(), 0);
			}
			for (MonitorEvent event : events) {
				Integer count = m.get(event.getType().getDisplayName());
				m.put(event.getType().getDisplayName(), count + 1);
			}
		} else {
			m.put("FETCH_EVENT_ERROR", 1);
		}
		return m;
	}
}
