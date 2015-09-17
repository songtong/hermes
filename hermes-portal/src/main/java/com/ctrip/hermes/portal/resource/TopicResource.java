package com.ctrip.hermes.portal.resource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.ConsumerView;
import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.bo.TopicView;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.CodecService;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.metaservice.service.SchemaService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.monitor.MonitorService;
import com.ctrip.hermes.producer.api.Producer;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicResource {
	private static final Logger log = LoggerFactory.getLogger(TopicResource.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private PortalMetaService metaService = PlexusComponentLocator.lookup(PortalMetaService.class);

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	private CodecService codecService = PlexusComponentLocator.lookup(CodecService.class);

	private MonitorService monitorService = PlexusComponentLocator.lookup(MonitorService.class);

	private PortalConfig config = PlexusComponentLocator.lookup(PortalConfig.class);

	private Pair<Boolean, ?> validateTopicView(TopicView topic) {
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
			} else if (topic.getStoragePartitionCount() < 5) {
				reason = "Database partition count should be bigger than 5";
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

		Topic topic = topicView.toMetaTopic();
		if (topicService.findTopicByName(topic.getName()) != null) {
			throw new RestException("Topic already exists.", Status.CONFLICT);
		}

		try {
			topicView = new TopicView(topicService.createTopic(topic));
		} catch (Exception e) {
			log.error("Create topic failed: {}.", content, e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
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

	@POST
	@Path("{topic}/sync")
	public Response syncTopic(@PathParam("topic") String topicName,
	      @QueryParam("force_schema") @DefaultValue("false") boolean forceSchema) {
		Topic topic = metaService.findTopicByName(topicName);
		if (topic == null) {
			throw new RestException(String.format("Topic %s is not found.", topicName), Status.NOT_FOUND);
		}
		WebTarget target = ClientBuilder.newClient().target("http://" + config.getSyncHost());
		boolean exist = false;
		try {
			exist = isTopicExistOnTarget(topicName, target);
		} catch (Exception e) {
			throw new RestException(String.format("Can not decide topic status: %s [ %s ] [ %s ]", topicName,
			      target.getUri(), e.getMessage()), Status.NOT_ACCEPTABLE);
		}
		if (exist && !forceSchema) {
			throw new RestException(String.format("Topic %s is already exists.", topicName), Status.CONFLICT);
		}

		TopicView view = getTopic(topicName);
		Set<String> missedDatasources = getMissedDatasourceOnTarget(view, target);
		if (missedDatasources.size() == 0) {
			switch (topic.getStorageType()) {
			case Storage.MYSQL:
				syncMysqlTopic(view, target);
				break;
			case Storage.KAFKA:
				syncKafkaTopic(view, target, exist, forceSchema);
				break;
			}
			syncConsumers(view, target);
		} else {
			throw new RestException("Target has missed datasources, pls init them: " + missedDatasources);
		}
		return Response.status(Status.OK).build();
	}

	private void syncConsumers(TopicView topic, WebTarget target) {
		for (ConsumerGroup consumer : metaService.findConsumersByTopic(topic.getName())) {
			Builder request = target.path("/api/consumers/add").request();
			ConsumerView requestView = new ConsumerView(topic.getName(), consumer);
			Response response = request.post(Entity.json(requestView));
			if (!(Status.CREATED.getStatusCode() == response.getStatus()//
			|| Status.CONFLICT.getStatusCode() == response.getStatus())) {
				throw new RestException(String.format("Add consumer %s failed.", consumer.getName()),
				      Status.INTERNAL_SERVER_ERROR);
			}
		}
	}

	private void syncMysqlTopic(TopicView topic, WebTarget target) {
		Builder request = target.path("/api/topics").request();
		TopicView view = null;
		try {
			view = request.post(Entity.json(topic), TopicView.class);
		} catch (Exception e) {
			throw new RestException(String.format("Sync mysql topic: %s failed: %s", topic.getName(), e.getMessage()),
			      Status.NOT_ACCEPTABLE);
		}
		if (view == null || !view.getName().equals(topic.getName())) {
			throw new RestException("Sync validation failed.", Status.INTERNAL_SERVER_ERROR);
		}
	}

	private void syncKafkaTopic(TopicView topic, WebTarget target, boolean alreadyExist, boolean forceSchema) {
		topic.setSchemaId(null);
		try {
			long targetTopicId = -1;
			if (!alreadyExist) {
				// create topic
				Builder request;
				TopicView view = createTopicOnTarget(topic, target);
				targetTopicId = view.getId();

				// deploy topic
				request = target.path(String.format("/api/topics/%s/deploy", topic.getName())).request();
				Response response = request.post(null);
				if (response.getStatus() != Status.OK.getStatusCode()) {
					String info = response.readEntity(String.class);
					log.warn("Deploy topic {} failed [{}]. Clean it from remote meta.", topic.getName(), info);
					deleteTopicMetaOnTarget(topic, target);
					throw new RestException("Deploy kafka topic failed: " + info, Status.INTERNAL_SERVER_ERROR);
				}
			} else if (forceSchema) {
				targetTopicId = getTopicOnTarget(topic.getName(), target).getId();
			}

			if (targetTopicId > -1) {
				// handle schemas
				syncSchema(topic, targetTopicId, target);
			}
		} catch (RestException e) {
			throw e;
		} catch (Exception e) {
			log.warn("Sync kafka topic failed.", e);
			throw new RestException(String.format("Sync kafka topic: %s failed: %s", topic.getName(), e.getMessage()),
			      Status.NOT_ACCEPTABLE);
		}
	}

	private TopicView createTopicOnTarget(TopicView topic, WebTarget target) {
		Builder request = target.path("/api/topics").request();
		TopicView view = request.post(Entity.json(topic), TopicView.class);
		if (view == null || !view.getName().equals(topic.getName())) {
			throw new RestException("Sync validation failed.", Status.INTERNAL_SERVER_ERROR);
		}
		return view;
	}

	private void syncSchema(TopicView topic, long targetTopicId, WebTarget target) {
		Builder request;
		Response response;
		File tmp = writeTempPreview(topic);
		if (tmp == null) {
			throw new RestException("Deploy schema failed, can not write tmp file.", Status.INTERNAL_SERVER_ERROR);
		}
		target.register(MultiPartFeature.class);
		request = target.path("/api/schemas").request();
		FormDataMultiPart form = new FormDataMultiPart();
		form.bodyPart(new FileDataBodyPart("file", tmp, MediaType.MULTIPART_FORM_DATA_TYPE));
		form.field("schema", JSON.toJSONString(topic.getSchema()));
		form.field("topicId", String.valueOf(targetTopicId));
		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		if (response.getStatus() != Status.CREATED.getStatusCode()) {
			log.warn("Deploy schema response: " + response);
			throw new RestException("Deploy schema failed.");
		}
	}

	private File writeTempPreview(TopicView topic) {
		try {
			File f = new File("/tmp", topic.getSchema().getName() + ".avsc");
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			bw.write(topic.getSchema().getSchemaPreview());
			bw.flush();
			bw.close();
			return f;
		} catch (IOException e) {
			log.warn("Write tmp schema preview failed.", e);
			return null;
		}
	}

	private boolean deleteTopicMetaOnTarget(TopicView topic, WebTarget target) {
		Builder request = target.path("/api/topics/" + topic.getName()).request();
		Response response = request.delete();
		return response.getStatus() == Status.OK.getStatusCode();
	}

	private Set<String> getMissedDatasourceOnTarget(TopicView topic, WebTarget target) {
		Set<String> iDses = new HashSet<String>();
		for (Partition partition : topic.getPartitions()) {
			iDses.add(partition.getReadDatasource());
			iDses.add(partition.getWriteDatasource());
		}
		Set<String> set = new HashSet<String>();
		Builder request = target.path("/api/meta/storages").queryParam("type", topic.getStorageType()).request();
		try {
			List<Storage> storages = request.get(new GenericType<List<Storage>>() {
			});
			for (Storage storage : storages) {
				if (storage.getType().equals(topic.getStorageType())) {
					for (Datasource ds : storage.getDatasources()) {
						if (iDses.contains(ds.getId())) {
							set.add(ds.getId());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RestException(
			      "Can not fetch remote datasource info, maybe api is not compatible: " + e.getMessage(),
			      Status.INTERNAL_SERVER_ERROR);
		}
		Set<String> ret = new HashSet<String>();
		for (String ds : iDses) {
			if (!set.contains(ds)) {
				ret.add(ds);
			}
		}
		return ret;
	}

	private boolean isTopicExistOnTarget(String topicName, WebTarget target) {
		TopicView topicView = getTopicOnTarget(topicName, target);
		return topicView != null && topicView.getName().equals(topicName);
	}

	private TopicView getTopicOnTarget(String topicName, WebTarget target) {
		Builder request = target.path("/api/topics/" + topicName).request();
		try {
			return request.get(TopicView.class);
		} catch (Exception e) {
			return null;
		}
	}

	@GET
	public List<TopicView> findTopics(@QueryParam("pattern") String pattern, @QueryParam("type") String type) {
		log.debug("find topics, pattern {}", pattern);
		if (StringUtils.isEmpty(pattern)) {
			pattern = ".*";
		}

		List<Topic> topics = topicService.findTopics(pattern);
		List<TopicView> returnResult = new ArrayList<TopicView>();
		try {
			for (Topic topic : topics) {
				TopicView topicView = prepareTopicView(topic);

				Storage storage = metaService.findStorageByTopic(topic.getName());
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
				if (type != null && type.equals(topicView.getStorageType())) {
					returnResult.add(topicView);
				} else if (type == null) {
					returnResult.add(topicView);
				}
			}
		} catch (Exception e) {
			log.warn("find topics failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	private TopicView prepareTopicView(Topic topic) {
		TopicView topicView = new TopicView(topic);
		long delay = monitorService.getDelay(topic.getName());
		topicView.setTotalDelay(delay);
		topicView.setLatestProduced(monitorService.getLatestProduced(topic.getName()));
		return topicView;
	}

	@GET
	@Path("{name}")
	public TopicView getTopic(@PathParam("name") String name) {
		log.debug("get topic {}", name);
		Topic topic = topicService.findTopicByName(name);
		if (topic == null) {
			throw new RestException("Topic not found: " + name, Status.NOT_FOUND);
		}

		TopicView topicView = prepareTopicView(topic);

		// Fill Storage
		Storage storage = metaService.findStorageByTopic(topic.getName());
		topicView.setStorage(storage);

		// Fill Schema
		if (topic.getSchemaId() != null) {
			SchemaView schemaView;
			try {
				schemaView = schemaService.getSchemaView(topic.getSchemaId());
				topicView.setSchema(schemaView);
			} catch (Exception e) {
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
		log.debug("get schemas, name: {}", name);
		List<SchemaView> returnResult = null;
		TopicView topic = getTopic(name);
		try {
			returnResult = schemaService.listSchemaView(topic.toMetaTopic());
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

		Topic topic = topicView.toMetaTopic();

		if (topicService.findTopicByName(topic.getName()) == null) {
			throw new RestException("Topic does not exists.", Status.NOT_FOUND);
		}
		try {
			topic = topicService.updateTopic(topic);
			topicView = new TopicView(topic);
		} catch (Exception e) {
			log.warn("update topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(topicView).build();
	}

	@POST
	@Path("{name}/partition/add")
	public Response addPartitionForTopic(@PathParam("name") String name, String content) {
		log.debug("add partition {} for topic {}", content, name);
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		Partition partition = null;
		try {
			partition = JSON.parseObject(content, Partition.class);
		} catch (Exception e) {
			log.warn("parse partition failed", e);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		if (topicService.findTopicByName(name) == null) {
			throw new RestException("Topic does not exists.", Status.NOT_FOUND);
		}

		Topic topic = null;
		TopicView topicView = null;
		try {
			topic = topicService.addPartitionForTopic(name, partition);
			topicView = new TopicView(topic);
		} catch (Exception e) {
			log.warn("add topic partition failed", e);
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

	@POST
	@Path("{name}/deploy")
	public Response deployTopic(@PathParam("name") String name) {
		log.debug("deploy {}", name);
		TopicView topicView = getTopic(name);
		try {
			Topic topic = topicView.toMetaTopic();
			if ("kafka".equalsIgnoreCase(topic.getStorageType())) {
				topicService.createTopicInKafka(topic);
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
			Topic topic = topicView.toMetaTopic();
			if ("kafka".equalsIgnoreCase(topic.getStorageType())) {
				topicService.deleteTopicInKafka(topic);
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
			Topic topic = topicView.toMetaTopic();
			if ("kafka".equalsIgnoreCase(topic.getStorageType())) {
				topicService.configTopicInKafka(topic);
			}
		} catch (Exception e) {
			log.warn("config topic failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}
}
