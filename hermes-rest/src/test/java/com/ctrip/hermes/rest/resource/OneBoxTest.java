package com.ctrip.hermes.rest.resource;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.rest.TestGatewayServer;
import com.google.common.base.Charsets;

@Path("/onebox")
public class OneBoxTest extends ComponentTestCase {

	private static List<byte[]> sent = new ArrayList<>();

	private static List<byte[]> received = new ArrayList<>();

	private static TestGatewayServer server;

	@BeforeClass
	public static void startServer() throws Exception {
		server = new TestGatewayServer();
		server.startServer();
	}

	@AfterClass
	public static void stopServer() throws Exception {
		server.stopServer();
	}

	@Test
	public void testOneBox() throws InterruptedException, IOException {
		Client client = ClientBuilder.newClient();
		WebTarget portalWebTarget = client.target(TestGatewayServer.PORTAL_HOST);
		WebTarget gatewayWebTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTopic";
		String group = "OneBoxGroup";
		String urls = "http://localhost:1357/onebox/push1,http://localhost:1357/onebox/push2";

		SubscriptionView sub = new SubscriptionView();
		sub.setTopic(topic);
		sub.setGroup(group);
		sub.setEndpoints(urls);
		sub.setName(UUID.randomUUID().toString());

		Builder request = portalWebTarget.path("/api/subscriptions/").request();
		String json = JSON.toJSONString(sub);
		System.out.println("Post: " + json);
		Response response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
		Assert.assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
		SubscriptionView createdSub = response.readEntity(SubscriptionView.class);
		Long id = createdSub.getId();

		System.out.println("Sleep 10 seconds");
		TimeUnit.SECONDS.sleep(10);

		String base = UUID.randomUUID().toString();
		System.out.println("Base: " + base);

		int i = 0;
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				byte[] msg = ("Hello World " + base + " " + i++).getBytes();
				sent.add(msg);
				request = gatewayWebTarget.path("topics/" + topic).request();
				InputStream is = new ByteArrayInputStream(msg);
				response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
				Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
				System.out.println("Sent: " + new String(msg));
			}
		}

		System.out.println(sent);
		System.out.println(received);

		request = portalWebTarget.path("/api/subscriptions/" + id).request();
		response = request.delete();
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

		Assert.assertEquals(sent.size(), received.size());
		Assert.assertArrayEquals(sent.get(0), received.get(0));
		Assert.assertArrayEquals(sent.get(sent.size() - 1), received.get(received.size() - 1));
	}

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
