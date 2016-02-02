package com.ctrip.hermes.portal.meta.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.metaservice.view.CodecView;
import com.ctrip.hermes.portal.StartPortal;

public class CodecResourceTest extends StartPortal {

	@After
	public void stop() throws Exception {
		stopServer();
	}

	@Test
	public void testGetCodec() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://localhost:" + getServerPort());
		String topic = "kafka.SimpleTopic";
		Builder request = webTarget.path("/api/codecs/" + topic).request();
		Response response = request.get();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
		CodecView codec = response.readEntity(CodecView.class);
		System.out.println(codec);
		List<String> codecs = new ArrayList<>();
		codecs.add("json");
		codecs.add("avro");
		Assert.assertTrue(codecs.contains(codec.getType()));

		request = webTarget.path("/api/codecs/" + UUID.randomUUID()).request();
		response = request.get();
		Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
	}

	@Test
	public void testListCodecs() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://localhost:" + getServerPort());
		Builder request = webTarget.path("/api/codecs/").request();
		Response response = request.get();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());

		List<CodecView> readEntity = response.readEntity(new GenericType<List<CodecView>>() {
		});
		List<String> codecs = new ArrayList<>();
		codecs.add("json");
		codecs.add("avro");
		codecs.add("cmessaging");
		for (CodecView codec : readEntity) {
			System.out.println(codec);
			Assert.assertTrue(codecs.contains(codec.getType()));
		}
	}
}
