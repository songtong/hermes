package com.ctrip.hermes.portal.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.converter.EntityToViewConverter;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.view.ConsumerView;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Named
public class SyncService {

	private static final Logger log = LoggerFactory.getLogger(SyncService.class);

	@Inject
	private ConsumerService consumerService;

	public void syncConsumers(TopicView topic, WebTarget target) {
		for (ConsumerGroup consumer : consumerService.getConsumers(topic.getName())) {
			consumer.setId(null);
			Builder request = target.path("/api/consumers/add").request();
			ConsumerView consumerView = EntityToViewConverter.convert(consumer);
			consumerView.setTopicName(topic.getName());
			Response response = request.post(Entity.json(consumerView));
			if (!(Status.CREATED.getStatusCode() == response.getStatus()//
			|| Status.CONFLICT.getStatusCode() == response.getStatus())) {
				throw new RestException(String.format("Add consumer %s failed.", consumer.getName()),
				      Status.INTERNAL_SERVER_ERROR);
			}
		}
	}

	public void syncMysqlTopic(TopicView topic, WebTarget target) {
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

	public void syncKafkaTopic(TopicView topic, WebTarget target, boolean alreadyExist, boolean forceSchema) {
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

			if (forceSchema && targetTopicId > -1) {
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
