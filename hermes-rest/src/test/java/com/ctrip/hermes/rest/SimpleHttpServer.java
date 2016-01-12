package com.ctrip.hermes.rest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;

public class SimpleHttpServer extends JerseyTest {
	
	@Test
	public void start() throws IOException {
		String timestamp = new SimpleDateFormat("MM-dd HH:mm:ss.SSS").format(new Date());
		System.out.println("BaseURI: " + getBaseUri());
		System.out.println(String.format("[%s] [INFO] Press any key to stop server ... ", timestamp));
		System.in.read();
	}

	@Override
	protected Application configure() {
		enable(TestProperties.LOG_TRAFFIC);
		enable(TestProperties.DUMP_ENTITY);
		set(TestProperties.CONTAINER_PORT, "8089");
		return new ResourceConfig(OneBoxResource.class);
	}
}
