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

import com.ctrip.hermes.kafka.admin.MockKafka;
import com.ctrip.hermes.kafka.admin.MockZookeeper;
import com.ctrip.hermes.kafka.producer.KafkaSendResult;
import com.ctrip.hermes.rest.TestGatewayServer;

public class KafkaTopicsResourceTest {

	private MockZookeeper zk;

	private MockKafka kafka;

	private TestGatewayServer server;

	@Before
	public void before() throws Exception {
		zk = new MockZookeeper();
		kafka = new MockKafka();
		server = new TestGatewayServer();
		server.startServer();
	}

	@After
	public void after() throws Exception {
		server.stopServer();
		kafka.stop();
		zk.stop();
	}

	@Test
	public void testPostToKafka() throws IOException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content1 = "Hello World 1";
		InputStream is1 = new ByteArrayInputStream(content1.getBytes());
		Response response1 = request.post(Entity.entity(is1, MediaType.APPLICATION_OCTET_STREAM));
		is1.close();
		Assert.assertEquals(Status.OK.getStatusCode(), response1.getStatus());
		KafkaSendResult sendResult1 = response1.readEntity(KafkaSendResult.class);
		Assert.assertEquals(0, sendResult1.getOffset());

		String content2 = "Hello World 2";
		InputStream is2 = new ByteArrayInputStream(content2.getBytes());
		Response response2 = request.post(Entity.entity(is2, MediaType.APPLICATION_OCTET_STREAM));
		is2.close();
		Assert.assertEquals(Status.OK.getStatusCode(), response2.getStatus());
		KafkaSendResult sendResult2 = response2.readEntity(KafkaSendResult.class);
		Assert.assertEquals(1, sendResult2.getOffset());
	}

	@Test
	public void testPostToKafkaWithHeader() throws IOException {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		request.header("X-Hermes-Priority", "true");
		request.header("X-Hermes-Ref-Key", "mykey");
		request.header("X-Hermes-Partition-Key", "myPartition");
		request.header("X-Hermes-Properties", "key1=value1,key2=value2");
		String content = "Hello World " + System.currentTimeMillis();
		InputStream is = new ByteArrayInputStream(content.getBytes());
		Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
		is.close();
		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
		System.out.println(response.readEntity(String.class));
	}

	@Test
	public void testPostWrongTopic() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.WrongTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content = "Hello World " + System.currentTimeMillis();
		InputStream is = new ByteArrayInputStream(content.getBytes());
		Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
		Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
	}

	@Test
	public void testPostWrongType() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(TestGatewayServer.GATEWAY_HOST);

		String topic = "kafka.SimpleTextTopic";

		Builder request = webTarget.path("topics/" + topic).request();
		String content = "Hello World " + System.currentTimeMillis();
		Response response = request.post(Entity.entity(content, MediaType.TEXT_PLAIN));
		Assert.assertEquals(Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
	}
}
