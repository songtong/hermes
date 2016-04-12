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
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
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
	public List<ConsumerGroupView> getConsumers() {
		List<ConsumerGroupView> consumers = null;
		try {
			consumers = consumerService.getConsumerViews();
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}
		Collections.sort(consumers, new Comparator<ConsumerGroupView>() {
			@Override
			public int compare(ConsumerGroupView o1, ConsumerGroupView o2) {
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
	
	@POST
	@Path("{topic}/{consumer}/offset/{strategy}")
	public Response resetOffset(@PathParam("topic") String topicName, @PathParam("consumer") String consumerGroupName, @PathParam("strategy") String strategy, String content) {
		TopicView topic = topicService.findTopicViewByName(topicName);
		
		if (topic == null) {
			throw new RestException("Topic NOT found!", Status.NOT_FOUND);
		}
		
		ConsumerGroupView consumerGroupView = consumerService.findConsumerView(topic.getId(), consumerGroupName);
		
		if (consumerGroupView == null) {
			throw new RestException("Consumer group NOT found!", Status.NOT_FOUND);
		}
		
		System.out.println(content);
		
		return Response.status(Status.OK).build();
	}
	
	@GET
	@Path("{topic}")
	public List<ConsumerGroupView> getConsumers(@PathParam("topic") String topicName) {
		logger.debug("Get consumers of topic: {}", topicName);
		List<ConsumerGroupView> consumers = null;
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
			consumerService.deleteConsumerGroup(topic.getId(), consumer);
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

		ConsumerGroupView consumerView = null;
		try {
			consumerView = JSON.parseObject(content, ConsumerGroupView.class);
		} catch (Exception e) {
			logger.error("Parse consumer failed, content: {}", content, e);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		String consumerName = consumerView.getName();
		String topicName = consumerView.getTopicName();
		TopicView topicView = topicService.findTopicViewByName(topicName);
		if (consumerService.findConsumerGroupEntity(topicView.getId(), consumerName) != null) {
			throw new RestException("Consumer for " + topicName + " already exists.", Status.CONFLICT);
		}

		try {
			consumerView = consumerService.addConsumerGroup(topicView.getId(), consumerView);
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

		ConsumerGroupView consumerView = null;
		try {
			consumerView = JSON.parseObject(content, ConsumerGroupView.class);
		} catch (Exception e) {
			logger.error("Parse consumer failed, content:{}", content, e);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		try {
			TopicView topicView = topicService.findTopicViewByName(consumerView.getTopicName());
			consumerView = consumerService.updateConsumerGroup(topicView.getId(), consumerView);
		} catch (Exception e) {
			logger.warn("Update consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(consumerView).build();
	}
}
