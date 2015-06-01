package com.ctrip.hermes.rest.resource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.rest.HermesRestServer;
import com.ctrip.hermes.rest.StartRestServer;

public class TopicsResourceTest extends ComponentTestCase {

	private HermesRestServer server;

	@Before
	public void startServer() throws IOException {
		server = lookup(HermesRestServer.class);
		server.start();
	}

	@After
	public void stopServer() {
		server.stop();
	}

	@Test
	public void testPostStringToKafka() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StartRestServer.HOST);

		String topic = "kafka.SimpleTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		Response response = request.post(Entity.json("Hello World " + System.currentTimeMillis()));
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
	}

	@Test
	public void testPostBinaryToKafka() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StartRestServer.HOST);

		String topic = "kafka.SimpleTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content = "Hello World " + System.currentTimeMillis();
		InputStream is = new ByteArrayInputStream(content.getBytes());
		Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
	}

}
