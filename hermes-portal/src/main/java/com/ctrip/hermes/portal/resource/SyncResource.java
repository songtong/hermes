package com.ctrip.hermes.portal.resource;

import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.model.Schema;
import com.ctrip.hermes.metaservice.service.SchemaService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.SyncService;

@Path("/topics/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SyncResource {

	private static final Logger log = LoggerFactory.getLogger(SyncResource.class);

	private SyncService syncService = PlexusComponentLocator.lookup(SyncService.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	private PortalConfig config = PlexusComponentLocator.lookup(PortalConfig.class);

	@POST
	@Path("{topic}/sync")
	public Response syncTopic(@PathParam("topic") String topicName,
	      @QueryParam("force_schema") @DefaultValue("false") boolean forceSchema) {
		TopicView topic = topicService.findTopicViewByName(topicName);
		if (topic == null) {
			throw new RestException(String.format("Topic %s is not found.", topicName), Status.NOT_FOUND);
		}
		WebTarget target = ClientBuilder.newClient().target("http://" + config.getSyncHost());
		boolean exist = false;
		try {
			exist = syncService.isTopicExistOnTarget(topicName, target);
		} catch (Exception e) {
			throw new RestException(String.format("Can not decide topic status: %s [ %s ] [ %s ]", topicName,
			      target.getUri(), e.getMessage()), Status.NOT_ACCEPTABLE);
		}
		if (exist) {
			throw new RestException(String.format("Topic %s is already exists.", topicName), Status.CONFLICT);
		}

		Set<String> missedDatasources = syncService.getMissedDatasourceOnTarget(topic, target);
		long toicId = topic.getId();
		Long schemaId = topic.getSchemaId();
		topic.setId(null);
		topic.setSchemaId(null);
		if (missedDatasources.size() == 0) {
			switch (topic.getStorageType()) {
			case Storage.MYSQL:
				syncService.syncMysqlTopic(topic, target);
				break;
			case Storage.KAFKA:
				syncService.syncKafkaTopic(topic, target, exist, forceSchema);
				break;
			}
			topic.setId(toicId);

			if (schemaId != null) {
				try {
					syncService.syncSchema(schemaService.getSchemaMeta(schemaId), topicName, null, null, target);
				} catch (Exception e) {
					log.warn("Sync schema failed for topic {}", topic.getName(), e);
				}
			}
			syncService.syncConsumers(topic, target);
		} else {
			throw new RestException("Target has missed datasources, pls init them: " + missedDatasources);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("sync")
	public Response syncTopic(@QueryParam("environment") String environment,
	      @QueryParam("forceSchema") @DefaultValue("false") boolean forceSchema, String content) {
		log.info("Sync topic with payload {}.", content);
		if (StringUtils.isEmpty(content)) {
			log.error("Payload content is empty, sync topic failed.");
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Pair<Boolean, ?> result = null;
		try {
			result = TopicResource.validateTopicView(JSON.parseObject(content, TopicView.class));
		} catch (Exception e) {
			log.error("Can not parse payload: {}, sync topic failed.", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		if (!result.getKey()) {
			throw new RestException((String) result.getValue());
		}

		TopicView topicView = (TopicView) result.getValue();

		WebTarget target = null;
		if ("uat".equals(environment)) {
			target = ClientBuilder.newClient().target("http://" + config.getPortalUatHost());
		} else if ("prod".equals(environment)) {
			target = ClientBuilder.newClient().target("http://" + config.getPortalProdHost());
		} else {
			throw new RestException("Unvalid environment:" + environment + "!", Status.BAD_REQUEST);
		}
		boolean exist = false;
		try {
			exist = syncService.isTopicExistOnTarget(topicView.getName(), target);
		} catch (Exception e) {
			throw new RestException(String.format("Can not decide topic status: %s [ %s ] [ %s ]", topicView.getName(),
			      target.getUri(), e.getMessage()), Status.NOT_ACCEPTABLE);
		}
		if (exist) {
			throw new RestException(String.format("Topic %s is already exists.", topicView.getName()), Status.CONFLICT);
		}

		Set<String> missedDatasources = syncService.getMissedDatasourceOnTarget(topicView, target);
		if (missedDatasources.size() == 0) {
			switch (topicView.getStorageType()) {
			case Storage.MYSQL:
				syncService.syncMysqlTopic(topicView, target);
				break;
			case Storage.KAFKA:
				syncService.syncKafkaTopic(topicView, target, exist, forceSchema);
				break;
			}
			// syncService.syncConsumers(topicView, target);
		} else {
			throw new RestException("Target has missed datasources, pls init them: " + missedDatasources);
		}
		return Response.status(Status.OK).build();
	}

	@POST
	@Path("{topic}/syncSchema/{userName}/{userEmail}")
	public Response syncSchema(@PathParam("topic") String topicName, @PathParam("userName") String userName,
	      @PathParam("userEmail") String userEmail) {
		TopicView topic = topicService.findTopicViewByName(topicName);
		if (topic == null) {
			throw new RestException(String.format("Topic %s is not found.", topicName), Status.NOT_FOUND);
		}
		WebTarget target = ClientBuilder.newClient().target("http://" + config.getSyncHost());
		boolean exist = false;
		try {
			exist = syncService.isTopicExistOnTarget(topicName, target);
		} catch (Exception e) {
			throw new RestException(String.format("Can not decide topic status: %s [ %s ] [ %s ]", topicName,
			      target.getUri(), e.getMessage()), Status.NOT_ACCEPTABLE);
		}
		if (!exist) {
			throw new RestException(String.format("Topic %s not exists in target environment.", topicName),
			      Status.NOT_FOUND);
		}
		Schema schema;
		try {
			schema = schemaService.getSchemaMeta(topic.getSchemaId());
		} catch (DalException e) {
			throw new RestException(String.format("Schema %s not found.", topic.getSchema().getName()), Status.NOT_FOUND);
		}
		syncService.syncSchema(schema, topicName, userName, userEmail, target);
		return Response.status(Status.OK).build();
	}

}
