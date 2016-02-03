package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.converter.EntityToViewConverter;
import com.ctrip.hermes.metaservice.converter.ViewToEntityConverter;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.view.ConsumerView;
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
		List<ConsumerView> returnResult = new ArrayList<ConsumerView>();
		try {
			Map<String, List<ConsumerGroup>> consumers = consumerService.getConsumers();
			for (Entry<String, List<ConsumerGroup>> entry : consumers.entrySet()) {
				for (ConsumerGroup consumer : entry.getValue()) {
					ConsumerView view = EntityToViewConverter.convert(consumer);
					view.setTopicName(entry.getKey());
					returnResult.add(view);
				}
			}
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}
		Collections.sort(returnResult, new Comparator<ConsumerView>() {
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
		return returnResult;
	}

	@GET
	@Path("{topic}")
	public List<ConsumerView> getConsumers(@PathParam("topic") String topic) {
		logger.debug("Get consumers of topic: {}", topic);
		List<ConsumerView> returnResult = new ArrayList<ConsumerView>();
		try {
			List<ConsumerGroup> consumers = consumerService.getConsumers(topic);
			for (ConsumerGroup c : consumers) {
				ConsumerView view = EntityToViewConverter.convert(c);
				view.setTopicName(topic);
				returnResult.add(view);
			}
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}

		return returnResult;
	}

	@DELETE
	@Path("{topic}/{consumer}")
	public Response deleteConsumer(@PathParam("topic") String topicName, @PathParam("consumer") String consumer) {
		logger.debug("Delete consumer: {} {}", topicName, consumer);
		try {
			Topic topic = topicService.findTopicEntityByName(topicName);
			consumerService.deleteConsumerFromTopic(topic.getId(), consumer);
		} catch (Exception e) {
			logger.warn("delete topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("add")
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
		Topic topic = topicService.findTopicEntityByName(topicName);
		if (consumerService.findConsumerGroup(topic.getId(), consumerName) != null) {
			throw new RestException("Consumer for " + topicName + " already exists.", Status.CONFLICT);
		}

		try {
			consumerView = consumerService.addConsumerForTopics(topic.getId(), consumerView);
		} catch (Exception e) {
			logger.warn("Create consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(consumerView).build();
	}

	@POST
	@Path("update")
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

		String topicName = consumerView.getTopicName();
		ConsumerGroup consumerEntity = ViewToEntityConverter.convert(consumerView);
		try {
			Topic topic = topicService.findTopicEntityByName(consumerView.getTopicName());
			consumerEntity = consumerService.updateGroupForTopic(topic.getId(), consumerEntity);
		} catch (Exception e) {
			logger.warn("Update consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		consumerView = EntityToViewConverter.convert(consumerEntity);
		consumerView.setTopicName(topicName);
		return Response.status(Status.CREATED).entity(consumerView).build();
	}
}
