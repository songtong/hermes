package com.ctrip.hermes.rest;

import java.util.Properties;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class StartRestServer extends ComponentTestCase {

	public static String HOST = null;

	static {
		Properties load;
		load = PlexusComponentLocator.lookup(ClientEnvironment.class).getGlobalConfig();
		String port = load.getProperty("rest.port");
		HOST = "http://localhost:" + port + "/";
	}

	@Test
	public void start() throws Exception {
		HermesRestServer hermesRestServer = lookup(HermesRestServer.class);
		hermesRestServer.start();

		Thread.currentThread().join();
	}

}
