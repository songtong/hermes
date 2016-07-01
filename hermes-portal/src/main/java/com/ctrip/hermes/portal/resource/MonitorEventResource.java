package com.ctrip.hermes.portal.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;
import com.ctrip.hermes.metaservice.monitor.dao.MonitorEventStorage;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.resource.view.MonitorEventView;

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
				if (!MonitorEventType.CONSUME_LARGE_BACKLOG.equals(event.getType())) {
					Integer count = m.get(event.getType().getDisplayName());
					m.put(event.getType().getDisplayName(), count + 1);
				}
			}
		} else {
			m.put("FETCH_EVENT_ERROR", 1);
		}
		return m;
	}

	@GET
	@Path("detail")
	public Response findMonitorEvents( //
	      @QueryParam("pageCount") @DefaultValue("50") int pageCount, //
	      @QueryParam("pageNum") @DefaultValue("0") int pageOffset) {
		if (pageCount < 0 || pageOffset < 0) {
			throw new RestException("Invalid page count and page number.", Status.BAD_REQUEST);
		}
		@SuppressWarnings("unchecked")
		List<MonitorEventView> datas = (List<MonitorEventView>) CollectionUtil.collect(
		      m_eventStorage.findDBMonitorEvents(pageCount, pageOffset), new CollectionUtil.Transformer() {
			      @Override
			      public Object transform(Object input) {
				      return new MonitorEventView((com.ctrip.hermes.metaservice.model.MonitorEvent) input);
			      }
		      });
		long totalPage = m_eventStorage.totalPageCount(pageCount);
		return Response.status(Status.OK).entity(new QueryResult(totalPage, datas)).build();
	}

	@SuppressWarnings("unused")
	private static class QueryResult {
		private List<MonitorEventView> m_datas;

		private long m_totalPage;

		public QueryResult(long total, List<MonitorEventView> datas) {
			m_datas = datas;
			m_totalPage = total;
		}

		public List<MonitorEventView> getDatas() {
			return m_datas;
		}

		public long getTotalPage() {
			return m_totalPage;
		}
	}
}
