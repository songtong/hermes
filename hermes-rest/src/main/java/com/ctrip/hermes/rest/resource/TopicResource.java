package com.ctrip.hermes.rest.resource;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.storage.TopicStorageService;
import com.ctrip.hermes.core.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

// todo: merge into hermes-portal API
@Path("/topic")
public class TopicResource {

	TopicStorageService service = PlexusComponentLocator.lookup(TopicStorageService.class);

	@GET
	@Path("/createdb")
	@Produces(MediaType.APPLICATION_JSON)
	public Boolean createNewTopic(@QueryParam("ds") String ds,
											@QueryParam("name") String topicName) {
		try {
			return service.createNewTopic(ds, topicName);
		} catch (TopicAlreadyExistsException e) {
			throw new RuntimeException(e);
		}
	}
}
