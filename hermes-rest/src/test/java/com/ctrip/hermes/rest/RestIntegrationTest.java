package com.ctrip.hermes.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;
import com.ctrip.hermes.rest.service.SubscriptionRegisterService;
import com.google.common.base.Charsets;
import com.netflix.hystrix.Hystrix;

public class RestIntegrationTest extends JerseyTest {

	private static List<byte[]> sentContent = new ArrayList<>();

	private static List<byte[]> receivedContent = new ArrayList<>();

	private static List<Map<String, String>> receivedHeaders = new ArrayList<>();

	private static MockZookeeper zk;

	private static MockKafka kafka;

	private static TestGatewayServer gatewayServer;

	@BeforeClass
	public static void beforeClass() throws Exception {
		zk = new MockZookeeper();
		kafka = new MockKafka(zk);
		gatewayServer = new TestGatewayServer();
		gatewayServer.startServer();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		gatewayServer.stopServer();
		kafka.stop();
		zk.stop();
	}

	@After
	public void after() {
		sentContent.clear();
		receivedContent.clear();
		receivedHeaders.clear();
		Hystrix.reset();
		HermesMetricsRegistry.reset();
	}

	@Test
	public void testPushSuccess() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";
		kafka.createTopic(topic);
		String group = "SimpleTextTopicGroup";
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
			byte[] msg = ("Hello World " + base + " " + (++i)).getBytes();
			sentContent.add(msg);
			Builder request = gatewayWebTarget.path("topics/" + topic).request();
			request.header("X-Hermes-Without-Header", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Partition-Key", String.valueOf(i));
			request.header("X-Hermes-Priority-Message", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Ref-Key", String.valueOf(i));
			request.header("X-Hermes-Message-Property", "pro1=value1,pro2=value2");
			InputStream is = new ByteArrayInputStream(msg);
			Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			is.close();
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
			System.out.println("Sent: " + new String(msg));
		}

		int sleepCount = 0;
		while (receivedContent.size() < sentContent.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		Assert.assertEquals(sentContent.size(), receivedContent.size());
		Assert.assertArrayEquals(sentContent.get(0), receivedContent.get(0));
		Assert.assertArrayEquals(sentContent.get(sentContent.size() - 1), receivedContent.get(receivedContent.size() - 1));

		i = 0;
		while (i < receivedHeaders.size()) {
			i++;
			Assert.assertEquals(String.valueOf(i), receivedHeaders.get(i - 1).get("X-Hermes-Ref-Key"));
			Assert.assertEquals(topic, receivedHeaders.get(i - 1).get("X-Hermes-Topic"));
		}

		registerService.stopSubscription(sub);
	}

	@Test
	public void testPushStandby() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic1";
		kafka.createTopic(topic);
		String group = "SimpleTextTopic1Group";
		String urls = "http://localhost:4321:/" + "onebox/pushNotExist," + getBaseUri() + "onebox/pushStandby";
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
			byte[] msg = ("Hello World " + base + " " + (++i)).getBytes();
			sentContent.add(msg);
			Builder request = gatewayWebTarget.path("topics/" + topic).request();
			request.header("X-Hermes-Without-Header", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Partition-Key", String.valueOf(i));
			request.header("X-Hermes-Priority-Message", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Ref-Key", String.valueOf(i));
			request.header("X-Hermes-Message-Property", "pro1=value1,pro2=value2");
			InputStream is = new ByteArrayInputStream(msg);
			Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			is.close();
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
			System.out.println("Sent: " + new String(msg));
		}

		int sleepCount = 0;
		while (receivedContent.size() < sentContent.size() && sleepCount++ < 150) {
			Thread.sleep(100);
		}

		Assert.assertEquals(sentContent.size(), receivedContent.size());
		Assert.assertArrayEquals(sentContent.get(0), receivedContent.get(0));
		Assert.assertArrayEquals(sentContent.get(sentContent.size() - 1), receivedContent.get(receivedContent.size() - 1));

		i = 0;
		while (i < receivedHeaders.size()) {
			i++;
			Assert.assertEquals(String.valueOf(i), receivedHeaders.get(i - 1).get("X-Hermes-Ref-Key"));
			Assert.assertEquals(topic, receivedHeaders.get(i - 1).get("X-Hermes-Topic"));
		}

		registerService.stopSubscription(sub);
	}

	@Test
	public void testPushNotExistEndpoint() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic2";
		kafka.createTopic(topic);
		String group = "SimpleTextTopic2Group";
		String urls = getBaseUri() + "onebox/pushNotExist";

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
			byte[] msg = ("Hello World " + base + " " + (++i)).getBytes();
			sentContent.add(msg);
			Builder request = gatewayWebTarget.path("topics/" + topic).request();
			request.header("X-Hermes-Without-Header", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Partition-Key", String.valueOf(i));
			request.header("X-Hermes-Priority-Message", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Ref-Key", String.valueOf(i));
			request.header("X-Hermes-Message-Property", "pro1=value1,pro2=value2");
			InputStream is = new ByteArrayInputStream(msg);
			Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			is.close();
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
			System.out.println("Sent: " + new String(msg));
		}

		int sleepCount = 0;
		while (receivedContent.size() < sentContent.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		Assert.assertEquals(0, receivedContent.size());

		registerService.stopSubscription(sub);
	}

	@Test
	public void testPushTimeoutEndpoint() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic4";
		kafka.createTopic(topic);
		String group = "SimpleTextTopic4Group";
		String urls = getBaseUri() + "onebox/pushTimeout";

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
			byte[] msg = ("Hello World " + base + " " + (++i)).getBytes();
			sentContent.add(msg);
			Builder request = gatewayWebTarget.path("topics/" + topic).request();
			request.header("X-Hermes-Without-Header", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Partition-Key", String.valueOf(i));
			request.header("X-Hermes-Priority-Message", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Ref-Key", String.valueOf(i));
			request.header("X-Hermes-Message-Property", "pro1=value1,pro2=value2");
			InputStream is = new ByteArrayInputStream(msg);
			Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			is.close();
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
			System.out.println("Sent: " + new String(msg));
		}

		int sleepCount = 0;
		while (receivedContent.size() < sentContent.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		Assert.assertEquals(0, receivedContent.size());

		registerService.stopSubscription(sub);
	}

	@Test
	public void testPushNotAvailableEndpoint() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic3";
		kafka.createTopic(topic);
		String group = "SimpleTextTopic3Group";
		String urls = getBaseUri() + "onebox/pushNotAvailable";

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
			byte[] msg = ("Hello World " + base + " " + (++i)).getBytes();
			sentContent.add(msg);
			Builder request = gatewayWebTarget.path("topics/" + topic).request();
			request.header("X-Hermes-Without-Header", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Partition-Key", String.valueOf(i));
			request.header("X-Hermes-Priority-Message", i % 2 == 0 ? "true" : "false");
			request.header("X-Hermes-Ref-Key", String.valueOf(i));
			request.header("X-Hermes-Message-Property", "pro1=value1,pro2=value2");
			InputStream is = new ByteArrayInputStream(msg);
			Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			is.close();
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
			System.out.println("Sent: " + new String(msg));
		}

		int sleepCount = 0;
		while (receivedContent.size() < sentContent.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		Assert.assertEquals(0, receivedContent.size());

		registerService.stopSubscription(sub);
	}

	@Path("onebox")
	public static class OneBoxResource {

		@Path("hello")
		@GET
		public String hello() {
			return "Hello World";
		}

		@Path("push")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response push(@Context HttpHeaders headers, byte[] b) {
			System.out.println("Received: " + new String(b, Charsets.UTF_8));
			receivedContent.add(b);
			Map<String, String> receivedHeader = new HashMap<>();
			receivedHeader.put("X-Hermes-Topic", headers.getHeaderString("X-Hermes-Topic"));
			receivedHeader.put("X-Hermes-Ref-Key", headers.getHeaderString("X-Hermes-Ref-Key"));
			receivedHeaders.add(receivedHeader);
			return Response.ok().build();
		}

		@Path("pushNotAvailable")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response pushServerError(byte[] b) {
			return Response.serverError().build();
		}

		@Path("pushTimout")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response pushTimeout(byte[] b) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
			return Response.ok().build();
		}

		@Path("pushStandby")
		@POST
		@Consumes(MediaType.APPLICATION_OCTET_STREAM)
		public Response pushStandby(@Context HttpHeaders headers, byte[] b) {
			System.out.println("Received: " + new String(b, Charsets.UTF_8));
			receivedContent.add(b);
			Map<String, String> receivedHeader = new HashMap<>();
			receivedHeader.put("X-Hermes-Topic", headers.getHeaderString("X-Hermes-Topic"));
			receivedHeader.put("X-Hermes-Ref-Key", headers.getHeaderString("X-Hermes-Ref-Key"));
			receivedHeaders.add(receivedHeader);
			return Response.ok().build();
		}
	}

	@Override
	protected Application configure() {
		enable(TestProperties.LOG_TRAFFIC);
		enable(TestProperties.DUMP_ENTITY);
		return new ResourceConfig(OneBoxResource.class);
	}
}
