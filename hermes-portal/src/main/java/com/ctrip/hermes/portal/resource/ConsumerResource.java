package com.ctrip.hermes.portal.resource;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.view.ConsumerView;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/consumers/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ConsumerResource {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerResource.class);

	private ConsumerService consumerService = PlexusComponentLocator.lookup(ConsumerService.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	@GET
	public List<ConsumerView> getConsumers() {
		List<ConsumerView> consumers = null;
		try {
			consumers = consumerService.getConsumerViews();
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}
		Collections.sort(consumers, new Comparator<ConsumerView>() {
			@Override
			public int compare(ConsumerView o1, ConsumerView o2) {
				int ret = 0;
				if (!StringUtils.isEmpty(o1.getName()) && !StringUtils.isEmpty(o2.getName()))
					ret = ret == 0 ? o1.getName().compareTo(o2.getName()) : ret;
				if (!StringUtils.isEmpty(o1.getAppIds()) && !StringUtils.isEmpty(o2.getAppIds()))
					ret = ret == 0 ? o1.getAppIds().compareTo(o2.getAppIds()) : ret;
				if (!StringUtils.isEmpty(o1.getTopicName()) && !StringUtils.isEmpty(o2.getTopicName()))
					ret = ret == 0 ? o1.getTopicName().compareTo(o2.getTopicName()) : ret;
				return ret;
			}
		});
		return consumers;
	}

	@GET
	@Path("{topic}")
	public List<ConsumerView> getConsumers(@PathParam("topic") String topicName) {
		logger.debug("Get consumers of topic: {}", topicName);
		List<ConsumerView> consumers = null;
		try {
			TopicView topic = topicService.findTopicViewByName(topicName);
			consumers = consumerService.findConsumerViews(topic.getId());
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}

		return consumers;
	}

	@DELETE
	@Path("{topic}/{consumer}")
	public Response deleteConsumer(@PathParam("topic") String topicName, @PathParam("consumer") String consumer) {
		logger.debug("Delete consumer: {} {}", topicName, consumer);
		try {
			TopicView topic = topicService.findTopicViewByName(topicName);
			consumerService.deleteConsumerFromTopic(topic.getId(), consumer);
		} catch (Exception e) {
			logger.warn("delete topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	public Response addConsumer(String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		ConsumerView consumerView = null;
		try {
			consumerView = JSON.parseObject(content, ConsumerView.class);
		} catch (Exception e) {
			logger.error("Parse consumer failed, content: {}", content, e);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		String consumerName = consumerView.getName();
		String topicName = consumerView.getTopicName();
		TopicView topicView = topicService.findTopicViewByName(topicName);
		if (consumerService.findConsumerGroup(topicView.getId(), consumerName) != null) {
			throw new RestException("Consumer for " + topicName + " already exists.", Status.CONFLICT);
		}

		try {
			consumerView = consumerService.addConsumerForTopics(topicView.getId(), consumerView);
		} catch (Exception e) {
			logger.warn("Create consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(consumerView).build();
	}

	@PUT
	public Response updateConsumer(String content) {
		if (StringUtils.isEmpty(content))
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);

		ConsumerView consumerView = null;
		try {
			consumerView = JSON.parseObject(content, ConsumerView.class);
		} catch (Exception e) {
			logger.error("Parse consumer failed, content:{}", content, e);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		try {
			TopicView topicView = topicService.findTopicViewByName(consumerView.getTopicName());
			consumerView = consumerService.updateGroupForTopic(topicView.getId(), consumerView);
		} catch (Exception e) {
			logger.warn("Update consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(consumerView).build();
	}
}
