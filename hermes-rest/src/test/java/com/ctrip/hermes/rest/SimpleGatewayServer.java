package com.ctrip.hermes.rest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ctrip.hermes.metrics.HermesMetricsRegistry;
import com.google.common.base.Charsets;
import com.netflix.hystrix.Hystrix;

public class SimpleGatewayServer extends JerseyTest {

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

//	@Test
	public void start() throws IOException {
		String timestamp = new SimpleDateFormat("MM-dd HH:mm:ss.SSS").format(new Date());
		System.out.println("BaseURI: "+getBaseUri());
		System.out.println(String.format("[%s] [INFO] Press any key to stop server ... ", timestamp));
		System.in.read();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		gatewayServer.stopServer();
		kafka.stop();
		zk.stop();
	}

	@After
	public void after() {
		Hystrix.reset();
		HermesMetricsRegistry.reset();
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
			Map<String, String> receivedHeader = new HashMap<>();
			receivedHeader.put("X-Hermes-Topic", headers.getHeaderString("X-Hermes-Topic"));
			receivedHeader.put("X-Hermes-Ref-Key", headers.getHeaderString("X-Hermes-Ref-Key"));
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
			Map<String, String> receivedHeader = new HashMap<>();
			receivedHeader.put("X-Hermes-Topic", headers.getHeaderString("X-Hermes-Topic"));
			receivedHeader.put("X-Hermes-Ref-Key", headers.getHeaderString("X-Hermes-Ref-Key"));
			return Response.ok().build();
		}
	}

	@Override
	protected Application configure() {
		enable(TestProperties.LOG_TRAFFIC);
		enable(TestProperties.DUMP_ENTITY);
		set(TestProperties.CONTAINER_PORT, "80");
		return new ResourceConfig(OneBoxResource.class);
	}
}
