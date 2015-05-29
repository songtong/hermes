package com.ctrip.hermes.portal.resource;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
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

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.dal.meta.Schema;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.pojo.SchemaView;
import com.ctrip.hermes.portal.pojo.TopicView;
import com.ctrip.hermes.portal.server.RestException;
import com.ctrip.hermes.portal.service.CodecService;
import com.ctrip.hermes.portal.service.SchemaService;
import com.ctrip.hermes.portal.service.TopicService;
import com.ctrip.hermes.portal.service.storage.TopicStorageService;
import com.ctrip.hermes.portal.service.storage.exception.TopicAlreadyExistsException;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicResource {

	private static final Logger logger = LoggerFactory.getLogger(TopicResource.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	private CodecService codecService = PlexusComponentLocator.lookup(CodecService.class);

	private TopicStorageService service = PlexusComponentLocator.lookup(TopicStorageService.class);

	@POST
	public Response createTopic(String content) {
		logger.debug("create topic, content {}", content);
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		TopicView topicView = null;
		try {
			topicView = JSON.parseObject(content, TopicView.class);
		} catch (Exception e) {
			logger.warn("parse topic failed, content: {}", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		Topic topic = topicView.toMetaTopic();

		if (topicService.getTopic(topic.getName()) != null) {
			throw new RestException("Topic already exists.", Status.CONFLICT);
		}
		try {
			topic = topicService.createTopic(topic);
			topicView = new TopicView(topic);
		} catch (Exception e) {
			logger.warn("create topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.CREATED).entity(topicView).build();
	}

	@GET
	@Path("{name}/createdb")
	@Produces(MediaType.APPLICATION_JSON)
	public Boolean createNewTopic(@QueryParam("ds") String ds, @PathParam("name") String topicName) {
		try {
			return service.createNewTopic(ds, topicName);
		} catch (TopicAlreadyExistsException e) {
			throw new RuntimeException(e);
		}
	}

	@GET
	public List<TopicView> findTopics(@QueryParam("pattern") String pattern) {
		logger.debug("find topics, pattern {}", pattern);
		if (StringUtils.isEmpty(pattern)) {
			pattern = ".*";
		}

		List<Topic> topics = topicService.findTopics(pattern);
		List<TopicView> returnResult = new ArrayList<TopicView>();
		try {
			for (Topic topic : topics) {
				TopicView topicView = new TopicView(topic);

				Storage storage = topicService.findStorage(topic.getName());
				topicView.setStorage(storage);

				if (topic.getSchemaId() != null) {
					try {
						SchemaView schemaView = schemaService.getSchemaView(topic.getSchemaId());
						topicView.setSchema(schemaView);
					} catch (DalNotFoundException e) {
					}
				}

				Codec codec = codecService.getCodec(topic.getName());
				topicView.setCodec(codec);

				returnResult.add(topicView);
			}
		} catch (DalException | IOException | RestClientException e) {
			logger.warn("find topics failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	@GET
	@Path("{name}")
	public TopicView getTopic(@PathParam("name") String name) {
		logger.debug("get topic {}", name);
		Topic topic = topicService.getTopic(name);
		if (topic == null) {
			throw new RestException("Topic not found: " + name, Status.NOT_FOUND);
		}

		TopicView topicView = new TopicView(topic);

		// Fill Storage
		Storage storage = topicService.findStorage(topic.getName());
		topicView.setStorage(storage);

		// Fill Schema
		if (topic.getSchemaId() != null) {
			SchemaView schemaView;
			try {
				schemaView = schemaService.getSchemaView(topic.getSchemaId());
				topicView.setSchema(schemaView);
			} catch (DalException | IOException | RestClientException e) {
				throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
			}
		}

		// Fill Codec
		Codec codec = codecService.getCodec(topic.getName());
		topicView.setCodec(codec);

		return topicView;
	}

	@GET
	@Path("{name}/schemas")
	public List<SchemaView> getSchemas(@PathParam("name") String name) {
		logger.debug("get schemas, name: {}", name);
		List<SchemaView> returnResult = new ArrayList<SchemaView>();
		TopicView topic = getTopic(name);
		try {
			List<Schema> schemaMetas = schemaService.listSchemaView(topic.toMetaTopic());
			for (Schema schema : schemaMetas) {
				SchemaView schemaView = new SchemaView(schema);
				returnResult.add(schemaView);
			}
		} catch (DalException e) {
			logger.warn("get schemas failed, name {}", name);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	@PUT
	@Path("{name}")
	public Response updateTopic(@PathParam("name") String name, String content) {
		logger.debug("update {} content {}", name, content);
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP PUT body is empty", Status.BAD_REQUEST);
		}
		TopicView topicView = null;
		try {
			topicView = JSON.parseObject(content, TopicView.class);
			topicView.setName(name);
		} catch (Exception e) {
			logger.warn("parse topic failed, content {}", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		Topic topic = topicView.toMetaTopic();

		if (topicService.getTopic(topic.getName()) == null) {
			throw new RestException("Topic does not exists.", Status.NOT_FOUND);
		}
		try {
			topic = topicService.updateTopic(topic);
			topicView = new TopicView(topic);
		} catch (Exception e) {
			logger.warn("update topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(topicView).build();
	}

	@DELETE
	@Path("{name}")
	public Response deleteTopic(@PathParam("name") String name) {
		logger.debug("delete {}", name);
		try {
			topicService.deleteTopic(name);
		} catch (Exception e) {
			logger.warn("delete topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{name}/deploy")
	public Response deployTopic(@PathParam("name") String name) {
		logger.debug("deploy {}", name);
		TopicView topicView = getTopic(name);
		try {
			Topic topic = topicView.toMetaTopic();
			if ("kafka".equalsIgnoreCase(topic.getStorageType())) {
				topicService.createTopicInKafka(topic);
			}
		} catch (Exception e) {
			logger.warn("deploy topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{name}/undeploy")
	public Response undeployTopic(@PathParam("name") String name) {
		logger.debug("undeploy {}", name);
		TopicView topicView = getTopic(name);
		try {
			Topic topic = topicView.toMetaTopic();
			if ("kafka".equalsIgnoreCase(topic.getStorageType())) {
				topicService.deleteTopicInKafka(topic);
			}
		} catch (Exception e) {
			logger.warn("undeploy topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{name}/config")
	public Response configTopic(@PathParam("name") String name) {
		logger.debug("config {}", name);
		TopicView topicView = getTopic(name);
		try {
			Topic topic = topicView.toMetaTopic();
			if ("kafka".equalsIgnoreCase(topic.getStorageType())) {
				topicService.configTopicInKafka(topic);
			}
		} catch (Exception e) {
			logger.warn("config topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}
}
