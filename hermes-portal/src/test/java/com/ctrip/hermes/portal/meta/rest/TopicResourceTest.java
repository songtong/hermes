package com.ctrip.hermes.portal.meta.rest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.client.Client;
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
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.bo.TopicView;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.portal.TestServer;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TopicResourceTest extends TestServer {

	private final String SERVER_HOST = "http://localhost:" + getServerPort();

	@After
	public void stop() throws Exception {
		stopServer();
	}

	@Test
	public void testGetTopic() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);
		String topic = "kafka.AvroTopic";
		Builder request = webTarget.path("/api/topics/" + topic).request();
		TopicView actual = request.get(TopicView.class);
		System.out.println(JSON.toJSONString(actual));
		Assert.assertEquals(topic, actual.getName());
	}

	@Test
	public void testListTopic() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);
		Builder request = webTarget.path("/api/topics/").queryParam("pattern", ".*").request();
		Response response = request.get();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
		List<TopicView> topics = response.readEntity(new GenericType<List<TopicView>>() {
		});
		System.out.println(topics);
		Assert.assertTrue(topics.size() > 0);
	}

	@Test
	public void testCreateExitingTopic() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);

		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);
		Builder request = webTarget.path("/api/topics/").request();
		Response response = request.post(Entity.json(topicView));
		Assert.assertEquals(Status.CONFLICT.getStatusCode(), response.getStatus());
	}

	@Test
	public void testCreateAndDeleteTopic() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);
		Builder request = webTarget.path("/api/topics/").request();
		Response response = request.post(Entity.json(topicView));
		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
		TopicView createdTopic = response.readEntity(TopicView.class);
		request = webTarget.path("/api/topics/" + createdTopic.getName()).request();
		response = request.delete();
		Assert.assertEquals(createdTopic.getEndpointType(), Endpoint.KAFKA);
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
	}

	@Test
	public void testUpdateTopic() throws IOException {
		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());

		ResourceConfig rc = new ResourceConfig();
		rc.register(MultiPartFeature.class);
		Client client = ClientBuilder.newClient(rc);
		WebTarget webTarget = client.target(SERVER_HOST);
		Builder request = webTarget.path("/api/topics/").request();
		Response response = request.post(Entity.json(topicView));
		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
		TopicView createdTopic = response.readEntity(TopicView.class);

		jsonString = Files.toString(new File("src/test/resources/schema-json-sample.json"), Charsets.UTF_8);
		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);

		FormDataMultiPart form = new FormDataMultiPart();
		File file = new File("src/test/resources/schema-json-sample.json");
		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
		form.field("schema", JSON.toJSONString(schemaView));
		form.field("topicId", String.valueOf(createdTopic.getId()));
		request = webTarget.path("/api/schemas/").request();
		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
		System.out.println(response.getStatus());
		SchemaView createdSchema = response.readEntity(SchemaView.class);

		request = webTarget.path("/api/topics/" + createdTopic.getName()).request();
		TopicView createdTopicWithSchema = request.get(TopicView.class);
		System.out.println(createdSchema.getId());
		Assert.assertEquals(createdSchema.getId(), createdTopicWithSchema.getSchemaId());

		createdTopicWithSchema.setDescription("Update Me");
		request = webTarget.path("/api/topics/" + createdTopicWithSchema.getName()).request();
		response = request.put(Entity.json(createdTopicWithSchema));
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
		TopicView updatedTopic = response.readEntity(TopicView.class);
		Assert.assertEquals("Update Me", updatedTopic.getDescription());
		Assert.assertEquals(createdSchema.getId(), updatedTopic.getSchemaId());

		request = webTarget.path("/api/schemas/" + createdSchema.getId()).request();
		response = request.delete();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());

		request = webTarget.path("/api/topics/" + createdTopic.getName()).request();
		response = request.delete();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
	}
}
