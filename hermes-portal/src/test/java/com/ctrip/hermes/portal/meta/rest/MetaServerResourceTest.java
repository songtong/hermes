package com.ctrip.hermes.portal.meta.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Test;

import com.ctrip.hermes.portal.TestServer;

public class MetaServerResourceTest extends TestServer {
	private final String SERVER_HOST = "http://localhost:" + getServerPort();

	@After
	public void stop() throws Exception {
		stopServer();
	}

	@Test
	public void testGetKafkaBrokers() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);
		Builder request = webTarget.path("metaserver/kafka/brokers").request();
		Response response = request.get();
		System.out.println(response.getStatus());
		System.out.println(response.readEntity(String.class));
	}

	@Test
	public void testGetZookeeper() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);
		Builder request = webTarget.path("metaserver/kafka/zookeeper").request();
		Response response = request.get();
		System.out.println(response.getStatus());
		System.out.println(response.readEntity(String.class));
	}
}
