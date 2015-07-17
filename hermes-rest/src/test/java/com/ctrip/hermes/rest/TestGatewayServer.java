package com.ctrip.hermes.rest;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestGatewayServer extends JettyServer {

	public static String GATEWAY_HOST = "http://localhost:1357";
	
	public static void main(String[] args) throws Exception {
		TestGatewayServer server = new TestGatewayServer();

		server.startServer();
		server.startWebapp();
		server.stopServer();
	}

	@Before
	public void startServer() throws Exception {
		// System.setProperty("devMode", "true");
		super.startServer();
	}

	@After
	public void stopServer() throws Exception {
		super.stopServer();
	}
	
	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return 1357;
	}

	@Override
	protected void postConfigure(WebAppContext context) {
	}

	@Test
	public void startWebapp() throws Exception {
		// open the page in the default browser
		waitForAnyKey();
	}
}
