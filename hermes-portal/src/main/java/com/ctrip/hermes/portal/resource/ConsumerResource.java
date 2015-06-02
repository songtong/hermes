package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.portal.pojo.ConsumerView;
import com.ctrip.hermes.portal.server.RestException;
import com.ctrip.hermes.portal.service.ConsumerService;

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

		return returnResult;
	}
}
