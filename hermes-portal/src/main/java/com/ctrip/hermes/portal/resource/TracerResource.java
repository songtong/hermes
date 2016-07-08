package com.ctrip.hermes.portal.resource;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.messageLifecycle.entity.MessageLifecycle;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.TracerService;

@Path("/tracer/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TracerResource {
	private static final Logger m_logger = LoggerFactory.getLogger(TracerResource.class);

	private TracerService m_tracerService = PlexusComponentLocator.lookup(TracerService.class);

	private TopicService m_topicService = PlexusComponentLocator.lookup(TopicService.class);

	@GET
	public Response trace(@QueryParam("topicName") String topicName, @QueryParam("refKey") String refKey,
	      @QueryParam("date") long date) {
		
		if (m_topicService.findTopicEntityByName(topicName) == null) {
			throw new RestException("Topic not found!", Status.BAD_REQUEST);
		}
		
		if (new Date().getTime() < date) {
			throw new RestException("Invalid date!", Status.BAD_REQUEST);
		}
		
		Date d = new Date(getDate(date));
		m_logger.info("Trace message with topic:{}, refKey:{}, date:{}", topicName, refKey, d);
		
		try {
			Pair<List<MessageLifecycle>, List<Pair<String, Object>>> result = m_tracerService.trace(topicName, d, refKey);
			String json = JSON.toJSONString(result);
			System.out.println(json);
			return Response.ok().entity(json).build();
		} catch (Exception e) {
			throw new RestException(e.getMessage(), Status.INTERNAL_SERVER_ERROR);
		}

	}

	private long getDate(long date) {
		date += TimeUnit.HOURS.toMillis(8);
		date = date - date % TimeUnit.DAYS.toMillis(1);
		date -= TimeUnit.HOURS.toMillis(8);
		return date;
	}

	public static void main(String[] args) {

	}

}
