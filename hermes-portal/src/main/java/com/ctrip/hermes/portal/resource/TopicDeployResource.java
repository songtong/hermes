package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.admin.core.service.TopicDeployService;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicDeployResource {

	private static final Logger log = LoggerFactory.getLogger(TopicDeployResource.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private TopicDeployService topicDeployService = PlexusComponentLocator.lookup(TopicDeployService.class);

	@POST
	@Path("{name}/deploy")
	public Response deployTopic(@PathParam("name") String name) {
		log.debug("deploy {}", name);
		TopicView topicView = getTopic(name);
		try {
			if ("kafka".equalsIgnoreCase(topicView.getStorageType())) {
				topicDeployService.createTopicInKafka(topicView);
			}
		} catch (Exception e) {
			log.warn("deploy topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{name}/undeploy")
	public Response undeployTopic(@PathParam("name") String name) {
		log.debug("undeploy {}", name);
		TopicView topicView = getTopic(name);
		try {
			if ("kafka".equalsIgnoreCase(topicView.getStorageType())) {
				topicDeployService.deleteTopicInKafka(topicView);
			}
		} catch (Exception e) {
			log.warn("undeploy topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{name}/config")
	public Response configTopic(@PathParam("name") String name) {
		log.debug("config {}", name);
		TopicView topicView = getTopic(name);
		try {
			if ("kafka".equalsIgnoreCase(topicView.getStorageType())) {
				topicDeployService.configTopicInKafka(topicView);
			}
		} catch (Exception e) {
			log.warn("config topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	private TopicView getTopic(@PathParam("name") String name) {
		log.debug("get topic {}", name);
		TopicView topicView = topicService.findTopicViewByName(name);
		if (topicView == null) {
			throw new RestException("Topic not found: " + name, Status.NOT_FOUND);
		}

		return topicView;
	}
}
