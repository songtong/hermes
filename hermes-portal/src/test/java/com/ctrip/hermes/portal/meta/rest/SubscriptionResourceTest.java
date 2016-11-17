package com.ctrip.hermes.portal.meta.rest;

import java.util.List;

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
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.admin.core.view.SubscriptionView;
import com.ctrip.hermes.portal.StartPortal;

public class SubscriptionResourceTest extends StartPortal {

	private final String SERVER_HOST = "http://localhost:" + getServerPort();
	
	@After
	public void stop() throws Exception {
		stopServer();
	}
	
	@Test
	public void testSubscribe() throws InterruptedException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(SERVER_HOST);

		String name = "myid";
		String topic = "kafka.SimpleTextTopic";
		String group = "OneBoxGroup";
		String urls = "http://localhost:1357/onebox";

		SubscriptionView sub = new SubscriptionView();
		sub.setTopic(topic);
		sub.setGroup(group);
		sub.setEndpoints(urls);
		sub.setName(name);
		sub.setStatus("RUNNING");

		Builder request = webTarget.path("subscriptions/").request();
		String json = JSON.toJSONString(sub);
		System.out.println("Post: " + json);
		Response response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
		Assert.assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
		
		sub = response.readEntity(SubscriptionView.class);

		request = webTarget.path("subscriptions/").request();
		response = request.get();
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
		List<SubscriptionView> subs = response.readEntity(new GenericType<List<SubscriptionView>>() {
		});
		System.out.println(subs.toString());

		request = webTarget.path("subscriptions/" + sub.getId()).request();
		response = request.delete();
		Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
	}

}
