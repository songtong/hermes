package com.ctrip.hermes.rest.resource;

import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Subscription;
import com.ctrip.hermes.rest.TestGatewayServer;

public class SubscriptionResourceTest extends ComponentTestCase {

	@Test
	public void testSubscribe() throws InterruptedException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.PORTAL_HOST);

		String id = "myid";
		String topic = "kafka.SimpleTopic";
		String group = "OneBoxGroup";
		String urls = "http://localhost:1357/onebox";

		Subscription sub = new Subscription();
		sub.setId(id);
		sub.setTopic(topic);
		sub.setGroup(group);
		sub.setEndpoints(urls);

		Builder request = webTarget.path("subscriptions/").request();
		String json = JSON.toJSONString(sub);
		System.out.println("Post: " + json);
		Response response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
		Assert.assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

		request = webTarget.path("subscriptions/").request();
		response = request.get();
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
		Map<String, Subscription> subs = response.readEntity(new GenericType<Map<String, Subscription>>() {
		});
		Assert.assertTrue(subs.containsKey(sub.getId()));
		System.out.println(subs.toString());

		request = webTarget.path("subscriptions/" + id).request();
		response = request.delete();
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
	}

}
