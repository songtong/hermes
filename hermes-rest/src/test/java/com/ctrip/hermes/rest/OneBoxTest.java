package com.ctrip.hermes.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;
import com.google.common.base.Charsets;

public class OneBoxTest extends JerseyTest {

	private static List<byte[]> sent = new ArrayList<>();

	private static List<byte[]> received = new ArrayList<>();

	private MockZookeeper zk;

	private MockKafka kafka;

	private TestGatewayServer gatewayServer;

	@Before
	public void before() throws Exception {
		zk = new MockZookeeper();
		kafka = new MockKafka();
		gatewayServer = new TestGatewayServer();
		gatewayServer.startServer();
	}

	@After
	public void after() throws Exception {
		gatewayServer.stopServer();
		kafka.stop();
		zk.stop();
	}

	@Test
	public void testOneBox() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";
		kafka.createTopic(topic);
		String group = "OneBoxGroup";
		String urls = getBaseUri() + "onebox/push";

		SubscriptionView sub = new SubscriptionView();
		sub.setTopic(topic);
		sub.setGroup(group);
		sub.setEndpoints(urls);
		sub.setName(UUID.randomUUID().toString());
		sub.setStatus("RUNNING");

		SubscriptionRegisterService registerService = PlexusComponentLocator.lookup(SubscriptionRegisterService.class);
		registerService.startSubscription(sub);

		String base = UUID.randomUUID().toString();
		System.out.println("Base: " + base);

		int msgSize = 10;
		int i = 0;
		while (i < msgSize) {
			byte[] msg = ("Hello World " + base + " " + i++).getBytes();
			sent.add(msg);
			Builder request = gatewayWebTarget.path("topics/" + topic).request();
			request.header("X-Hermes-Without-Header", "true");
			InputStream is = new ByteArrayInputStream(msg);
			Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			is.close();
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
			System.out.println("Sent: " + new String(msg));
		}

		// System.out.println(sent);
		// System.out.println(received);

		int sleepCount = 0;
		while (received.size() < sent.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		Assert.assertEquals(sent.size(), received.size());
		Assert.assertArrayEquals(sent.get(0), received.get(0));
		Assert.assertArrayEquals(sent.get(sent.size() - 1), received.get(received.size() - 1));
		kafka.deleteTopic(topic);
	}

	@Path("onebox")
	public static class OneBoxResource {

		@Path("push")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response push(byte[] b) {
			System.out.println("Received: " + new String(b, Charsets.UTF_8));
			received.add(b);
			return Response.ok().build();
		}

		@Path("push1")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response push1(byte[] b) {
			System.out.println("Received1: " + new String(b, Charsets.UTF_8));
			received.add(b);
			return Response.ok().build();
		}

		@Path("push2")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response push2(byte[] b) {
			System.out.println("Received2: " + new String(b, Charsets.UTF_8));
			received.add(b);
			return Response.ok().build();
		}
	}

	@Override
	protected Application configure() {
		return new ResourceConfig(OneBoxResource.class);
	}

}
