package com.ctrip.hermes.rest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ctrip.hermes.metrics.HermesMetricsRegistry;
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

	@Test
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

	@Override
	protected Application configure() {
		enable(TestProperties.LOG_TRAFFIC);
		enable(TestProperties.DUMP_ENTITY);
		set(TestProperties.CONTAINER_PORT, "80");
		return new ResourceConfig(OneBoxResource.class);
	}
}
