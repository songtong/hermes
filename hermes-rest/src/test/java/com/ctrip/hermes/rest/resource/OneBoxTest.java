package com.ctrip.hermes.rest.resource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.rest.TestServer;
import com.ctrip.hermes.rest.service.Subscription;
import com.google.common.base.Charsets;

@Path("/onebox")
public class OneBoxTest extends ComponentTestCase {

	private static List<byte[]> sent = new ArrayList<>();

	private static List<byte[]> received = new ArrayList<>();

	@Test
	public void testOneBox() throws InterruptedException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestServer.HOST);

		String topic = "kafka.SimpleTopic";
		String group = "OneBoxGroup";
		List<String> urls = Arrays.asList(new String[] { "http://localhost:1357/onebox" });

		Subscription sub = new Subscription();
		sub.setTopic(topic);
		sub.setGroupId(group);
		sub.setEndpoints(urls);

		Builder request = webTarget.path("subscriptions/" + topic + "/sub").request();
		String json = JSON.toJSONString(sub);
		System.out.println("Post: " + json);
		Response response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
		Assert.assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

		String base = UUID.randomUUID().toString();
		System.out.println("Base: " + base);
		for (int i = 0; i < 5; i++) {
			sent.add(("Hello World " + base + " " + i).getBytes());
			request = webTarget.path("topics/" + topic).request();

			InputStream is = new ByteArrayInputStream(sent.get(i));
			response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
		}

		while (received.size() < sent.size()) {
			TimeUnit.SECONDS.sleep(1);
			System.out.println("Received: " + received.size());
		}

		request = webTarget.path("subscriptions/" + topic + "/unsub").request();
		response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

		Assert.assertEquals(sent.size(), received.size());
		Assert.assertArrayEquals(sent.get(0), received.get(0));
		Assert.assertArrayEquals(sent.get(sent.size() - 1), received.get(received.size() - 1));
	}

	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response push(byte[] b) {
		System.out.println(new String(b, Charsets.UTF_8));
		received.add(b);
		return Response.ok().build();
	}

}
