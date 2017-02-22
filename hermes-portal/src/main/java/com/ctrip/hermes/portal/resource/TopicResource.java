package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.admin.core.converter.ViewToModelConverter;
import com.ctrip.hermes.admin.core.service.SchemaService;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.admin.core.view.SchemaView;
import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.dashboard.DashboardService;
import com.ctrip.hermes.portal.service.mail.PortalMailService;
import com.ctrip.hermes.producer.api.Producer;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicResource {
	private static final Logger log = LoggerFactory.getLogger(TopicResource.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	private DashboardService monitorService = PlexusComponentLocator.lookup(DashboardService.class);

	private PortalMailService m_mailService = PlexusComponentLocator.lookup(PortalMailService.class);

	static Pair<Boolean, ?> validateTopicView(TopicView topic) {
		boolean passed = true;
		String reason = "";
		if (StringUtils.isBlank(topic.getName())) {
			reason = "Topic name is required";
			passed = false;
		} else if (StringUtils.isBlank(topic.getStorageType())) {
			reason = "Storage type is required";
			passed = false;
		} else if (StringUtils.isBlank(topic.getEndpointType())) {
			switch (topic.getStorageType()) {
			case Storage.KAFKA:
				topic.setEndpointType(Endpoint.KAFKA);
				break;
			case Storage.MYSQL:
				topic.setEndpointType(Endpoint.BROKER);
				break;
			}
		}
		if (topic.getStorageType().equals(Storage.MYSQL)) {
			if (topic.getStoragePartitionSize() < 10000) {
				reason = "Database partition size should be bigger than 10000";
				passed = false;
			} else if (topic.getStoragePartitionCount() < 1) {
				reason = "Database partition count should be bigger than 1";
				passed = false;
			} else if (topic.getResendPartitionSize() < 500) {
				reason = "Resend partition count should be bigger than 500";
				passed = false;
			}
		}
		return new Pair<>(passed, passed ? topic : reason);
	}

	@POST
	public Response createTopic(String content) {
		log.info("Creating topic with payload {}.", content);
		if (StringUtils.isEmpty(content)) {
			log.error("Payload content is empty, create topic failed.");
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Pair<Boolean, ?> result = null;
		try {
			result = validateTopicView(JSON.parseObject(content, TopicView.class));
		} catch (Exception e) {
			log.error("Can not parse payload: {}, create topic failed.", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		if (!result.getKey()) {
			throw new RestException((String) result.getValue());
		}

		TopicView topicView = (TopicView) result.getValue();

		if (topicService.findTopicEntityByName(topicView.getName()) != null) {
			throw new RestException("Topic already exists.", Status.CONFLICT);
		}

		try {
			topicView = topicService.createTopic(topicView);
		} catch (Exception e) {
			log.error("Create topic failed: {}.", content, e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		if (topicView.getName().startsWith("fx.cat.log")) {
			try {
				m_mailService.sendCreateTopicFromCatMail(ViewToModelConverter.convert(topicView));
			} catch (Exception e) {
				log.warn("Send email of create topic({}) from cat failed.", topicView.getName(), e);
			}
		}

		return Response.status(Status.CREATED).entity(topicView).build();
	}

	@POST
	@Path("{topic}/send")
	public Response sendMessage(@PathParam("topic") String topic, String content) {
		try {
			Producer.getInstance().message(topic, "0", content).withRefKey(content).sendSync();
		} catch (MessageSendException e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@GET
	public Pair<Integer, List<TopicView>> findTopics(@QueryParam("type") String type,
	      @QueryParam("startPage") int startPage, @QueryParam("pageSize") @DefaultValue("15") int pageSize,
	      @QueryParam("filter") String filterContent) {
		log.debug("find topics, pattern {}", filterContent);

		try {
			TopicView filter = JSON.parseObject(filterContent, TopicView.class);
			if (filter == null) {
				filter = new TopicView();
			}
			filter.setStorageType(type);

			List<TopicView> topicViews = topicService.filterTopicViews(filter);
			List<TopicView> result = new ArrayList<>();
			int count = 0;
			for (int i = startPage * pageSize; i < topicViews.size(); i++) {
				TopicView topicView = topicViews.get(i);
				topicView.setLatestProduced(monitorService.getLatestProduced(topicView.getName()));
				result.add(topicView);
				count++;
				if (count >= pageSize) {
					break;
				}
			}
			return new Pair<Integer, List<TopicView>>(topicViews.size(), result);
		} catch (Exception e) {
			log.warn("find topics failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
	}

	@GET
	@Path("{name}")
	public TopicView getTopic(@PathParam("name") String name) {
		log.debug("get topic {}", name);
		TopicView topicView = topicService.findTopicViewByName(name);
		if (topicView == null) {
			throw new RestException("Topic not found: " + name, Status.NOT_FOUND);
		}

		topicView.setLatestProduced(monitorService.getLatestProduced(topicView.getName()));
		return topicView;
	}

	@GET
	@Path("names")
	public Response getTopicNames() {
		List<String> topicNames = topicService.getTopicNames();
		Collections.sort(topicNames);
		return Response.status(Status.OK).entity(topicNames).build();
	}

	@GET
	@Path("{name}/schemas")
	public List<SchemaView> getSchemas(@PathParam("name") String name) {
		log.debug("get schemas, name: {}", name);
		List<SchemaView> returnResult = null;
		TopicView topicView = getTopic(name);
		try {
			returnResult = schemaService.listSchemaView(topicView.getId());
		} catch (DalException e) {
			log.warn("get schemas failed, name {}", name);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	@PUT
	@Path("{name}")
	public Response updateTopic(@PathParam("name") String name, String content) {
		log.debug("update {} content {}", name, content);
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP PUT body is empty", Status.BAD_REQUEST);
		}
		TopicView topicView = null;
		try {
			topicView = JSON.parseObject(content, TopicView.class);
			topicView.setName(name);
		} catch (Exception e) {
			log.warn("parse topic failed, content {}", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		if (topicService.findTopicEntityByName(topicView.getName()) == null) {
			throw new RestException("Topic does not exists.", Status.NOT_FOUND);
		}
		try {
			topicView = topicService.updateTopic(topicView);
		} catch (Exception e) {
			log.warn("update topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(topicView).build();
	}

	@DELETE
	@Path("{name}")
	public Response deleteTopic(@PathParam("name") String name) {
		log.debug("delete {}", name);
		try {
			topicService.deleteTopic(name);
		} catch (Exception e) {
			log.warn("delete topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

}
