package com.ctrip.hermes.portal.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.Schema;
import com.ctrip.hermes.admin.core.service.ConsumerService;
import com.ctrip.hermes.admin.core.view.ConsumerGroupView;
import com.ctrip.hermes.admin.core.view.SchemaView;
import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Named
public class SyncService {

	private static final Logger log = LoggerFactory.getLogger(SyncService.class);

	@Inject
	private ConsumerService consumerService;

	public void syncConsumers(TopicView topic, WebTarget target) {
		try {
			for (ConsumerGroupView consumerView : consumerService.findConsumerViews(topic.getId())) {
				Builder request = target.path("/api/consumers/").request();
				consumerView.setId(null);
				Response response = request.post(Entity.json(consumerView));
				if (!(Status.CREATED.getStatusCode() == response.getStatus()//
				|| Status.CONFLICT.getStatusCode() == response.getStatus())) {
					throw new RestException(String.format("Add consumer %s failed.", consumerView.getName()),
					      Status.INTERNAL_SERVER_ERROR);
				}
			}
		} catch (Exception e) {
			log.warn("Sync Consumers failed.", e);
			throw new RestException(String.format("Sync consumers: %s failed: %s", topic.getName(), e.getMessage()),
			      Status.NOT_ACCEPTABLE);
		}
	}

	public void syncMysqlTopic(TopicView topic, WebTarget target) {
		createTopicOnTarget(topic, target);
	}

	public void syncKafkaTopic(TopicView topic, WebTarget target, boolean alreadyExist, boolean forceSchema) {
		try {
			// create topic
			Builder request;
			createTopicOnTarget(topic, target);

			// deploy topic
			request = target.path(String.format("/api/topics/%s/deploy", topic.getName())).request();
			Response response = request.post(null);
			if (response.getStatus() != Status.OK.getStatusCode()) {
				String info = response.readEntity(String.class);
				log.warn("Deploy topic {} failed [{}]. Clean it from remote meta.", topic.getName(), info);
				deleteTopicMetaOnTarget(topic, target);
				throw new RestException("Deploy kafka topic failed: " + info, Status.INTERNAL_SERVER_ERROR);
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
		return view;
	}

	public void syncSchema(Schema schema, String topicName, String userName, String userMail, WebTarget target) {
		schema.setName(null);
		schema.setTopicId(0);
		schema.setId(0);
		schema.setAvroid(null);
		Builder request = target.path(String.format("/api/schemas/%s", topicName)).queryParam("userName", userName)
		      .queryParam("userMail", userMail).request();
		SchemaView schemaView = null;
		try {
			schemaView = request.post(Entity.json(schema), SchemaView.class);
		} catch (Exception e) {
			throw new RestException(String.format("sync schema failed: %s", e.getMessage()), Status.NOT_ACCEPTABLE);
		}
		if (schemaView == null) {
			throw new RestException("Sync validation failed.", Status.INTERNAL_SERVER_ERROR);
		}
	}

	private boolean deleteTopicMetaOnTarget(TopicView topic, WebTarget target) {
		Builder request = target.path("/api/topics/" + topic.getName()).request();
		Response response = request.delete();
		return response.getStatus() == Status.OK.getStatusCode();
	}

	public Set<String> getMissedDatasourceOnTarget(TopicView topic, WebTarget target) {
		Set<String> iDses = new HashSet<String>();
		for (Partition partition : topic.getPartitions()) {
			iDses.add(partition.getReadDatasource());
			iDses.add(partition.getWriteDatasource());
		}
		Set<String> set = new HashSet<String>();
		Builder request = target.path("/api/storages").queryParam("type", topic.getStorageType()).request();
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

	public boolean isTopicExistOnTarget(String topicName, WebTarget target) {
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
}
