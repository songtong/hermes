package com.ctrip.hermes.rest.resource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.Path;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.rest.HermesRestServer;
import com.ctrip.hermes.rest.StartRestServer;
import com.ctrip.hermes.rest.service.Subscription;

@Path("/onebox")
public class SubscriptionResourceTest extends ComponentTestCase {

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
	public void testSubscribe() throws InterruptedException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(StartRestServer.HOST);

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

		request = webTarget.path("subscriptions/"+topic).request();
		response = request.get();
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
		List<Subscription> subs = response.readEntity(new GenericType<List<Subscription>>() {});
		Assert.assertTrue(subs.contains(sub));
		System.out.println(subs.toString());
		
		request = webTarget.path("subscriptions/" + topic + "/unsub").request();
		response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
	}

}
