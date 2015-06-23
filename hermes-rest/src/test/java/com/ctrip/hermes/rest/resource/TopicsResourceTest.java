package com.ctrip.hermes.rest.resource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.rest.TestGatewayServer;

public class TopicsResourceTest extends ComponentTestCase {

	@Test
	public void testPostToKafka() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content = "Hello World " + System.currentTimeMillis();
		InputStream is = new ByteArrayInputStream(content.getBytes());
		Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
	}

	@Test
	public void testPostToKafkaWithHeader() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		request.header("priority", "true");
		request.header("refKey", "mykey");
		request.header("partitionKey", "myPartition");
		request.header("properties", "key1=value1,key2=value2");
		String content = "Hello World " + System.currentTimeMillis();
		InputStream is = new ByteArrayInputStream(content.getBytes());
		Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
		System.out.println(response.readEntity(String.class));
	}

	@Test
	public void testPostWrongTopic() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.WrongTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content = "Hello World " + System.currentTimeMillis();
		InputStream is = new ByteArrayInputStream(content.getBytes());
		Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
		Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
	}

	@Test
	public void testPostWrongType() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content = "Hello World " + System.currentTimeMillis();
		Response response = request.post(Entity.entity(content, MediaType.TEXT_PLAIN));
		Assert.assertEquals(Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
	}
}
