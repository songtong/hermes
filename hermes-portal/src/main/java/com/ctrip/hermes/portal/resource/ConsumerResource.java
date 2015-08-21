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
import com.ctrip.hermes.core.bo.ConsumerView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/consumers/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ConsumerResource {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerResource.class);

	private ConsumerService consumerService = PlexusComponentLocator.lookup(ConsumerService.class);

	@GET
	public List<ConsumerView> getConsumers() {
		List<ConsumerView> returnResult = new ArrayList<ConsumerView>();
		try {
			Map<String, List<ConsumerGroup>> consumers = consumerService.getConsumers();
			for (Entry<String, List<ConsumerGroup>> entry : consumers.entrySet()) {
				for (ConsumerGroup consumer : entry.getValue()) {
					returnResult.add(new ConsumerView(entry.getKey(), consumer));
				}
			}
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}
		Collections.sort(returnResult, new Comparator<ConsumerView>() {
			@Override
			public int compare(ConsumerView o1, ConsumerView o2) {
				int ret = 0;
				if (!StringUtils.isEmpty(o1.getGroupName()) && !StringUtils.isEmpty(o2.getGroupName()))
					ret = ret == 0 ? o1.getGroupName().compareTo(o2.getGroupName()) : ret;
				if (!StringUtils.isEmpty(o1.getAppId()) && !StringUtils.isEmpty(o2.getAppId()))
					ret = ret == 0 ? o1.getAppId().compareTo(o2.getAppId()) : ret;
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
				returnResult.add(new ConsumerView(topic, c));
			}
		} catch (Exception e) {
			throw new RestException(e, Status.NOT_FOUND);
		}

		return returnResult;
	}

	@DELETE
	@Path("{topic}/{consumer}")
	public Response deleteConsumer(@PathParam("topic") String topic, @PathParam("consumer") String consumer) {
		logger.debug("Delete consumer: {} {}", topic, consumer);
		try {
			consumerService.deleteConsumerFromTopic(topic, consumer);
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
		String consumerName = consumerView.getGroupName();
		String topicName = consumerView.getTopicName();
			if (consumerService.getConsumer(topicName, consumerName) != null) {
				throw new RestException("Consumer for " + topicName + " already exists.", Status.CONFLICT);
			}

		ConsumerGroup c = consumerView.toMetaConsumer();
		try {
			c = consumerService.addConsumerForTopics(topicName,c);
		} catch (Exception e) {
			logger.warn("Create consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		consumerView = new ConsumerView(consumerView.getTopicName(), c);
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
			throw new RestException(e,Status.BAD_REQUEST);
		}
		
		ConsumerGroup c = consumerView.toMetaConsumer();
		try {
			c = consumerService.updateGroupForTopic(consumerView.getTopicName(),c);
		} catch (Exception e) {
			logger.warn("Update consumer failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		consumerView = new ConsumerView(consumerView.getTopicName(),c);
		return Response.status(Status.CREATED).entity(consumerView).build();
	}
}
