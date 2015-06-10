package com.ctrip.hermes.metaserver;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

@RunWith(JUnit4.class)
public class StartMetaServer extends JettyServer {
	public static void main(String[] args) throws Exception {
		StartMetaServer server = new StartMetaServer();

		server.startServer();
		server.startWebapp();
		server.stopServer();
	}

	@Before
	public void before() throws Exception {
		System.setProperty("devMode", "true");
		super.startServer();
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return PlexusComponentLocator.lookup(MetaServerConfig.class).getMetaServerPort();
	}

	@Override
	protected void postConfigure(WebAppContext context) {
		context.addFilter(GzipFilter.class, "/*", Handler.ALL);
	}

	@Test
	public void startWebapp() throws Exception {
		// open the page in the default browser
		waitForAnyKey();
	}
}
