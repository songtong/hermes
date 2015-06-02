package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.pojo.storage.TopicStorageView;
import com.ctrip.hermes.portal.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.exception.TopicAlreadyExistsException;

@Path("/storage/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicStorageResource {
	private DefaultTopicStorageService service = PlexusComponentLocator.lookup(DefaultTopicStorageService.class);

	@GET
	@Path("{name}/createdb")
	@Produces(MediaType.APPLICATION_JSON)
	public Boolean createNewTopic(@QueryParam("ds") String ds,
											@PathParam("name") String topicName) {
		try {
			return service.createNewTopic(ds, topicName);
		} catch (TopicAlreadyExistsException e) {
			//todo: handle exception
			throw new RuntimeException(e);
		} catch (StorageHandleErrorException e) {
			e.printStackTrace();
		}
		return false;
	}

	@GET
	@Path("{name}/info")
	@Produces(MediaType.APPLICATION_JSON)
	public TopicStorageView getTopicStorageByDS(@PathParam("name") String topicName, @QueryParam("ds") String ds) {
		try {
			return service.getTopicStorage(ds, topicName);
		} catch (Exception e) {
			//todo: handle exception
			throw new RuntimeException(e);
		}
	}

	@GET
	@Path("{name}/alter/addp")
	@Produces(MediaType.APPLICATION_JSON)
	public Boolean addPartition(@PathParam("name") String topicName, @QueryParam("ds") String ds) {
		//todo:考虑批量增加的需求
		try {
			return service.addPartition(ds, topicName);
		} catch (StorageHandleErrorException e) {
			//todo: handle exception
			throw new RuntimeException(e);
		}
	}

	@GET
	@Path("{name}/alter/delp")
	@Produces(MediaType.APPLICATION_JSON)
	public Boolean deletePartition(@PathParam("name") String topicName, @QueryParam("ds") String ds) {
		//todo:考虑批量删除的需求
		try {
			return service.deletePartition(ds, topicName);
		} catch (StorageHandleErrorException e) {
			//todo: handle exception
			throw new RuntimeException(e);
		}
	}
}
