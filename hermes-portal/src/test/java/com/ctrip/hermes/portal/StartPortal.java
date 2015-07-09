package com.ctrip.hermes.portal;

import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.unidal.net.Networks;
import org.unidal.test.jetty.JettyServer;

@RunWith(JUnit4.class)
public class StartPortal extends JettyServer {
	private TestingServer m_zkServer = null;

	public static void main(String[] args) throws Exception {
		StartPortal server = new StartPortal();

		server.startServer();
		server.startWebapp();
		server.stopServer();
	}

	@Before
	public void before() throws Exception {
		System.setProperty("devMode", "true");
		startServer();
	}

	@Override
	protected void startServer() throws Exception {
		String zkMode = System.getProperty("zkMode");
		if (!"real".equalsIgnoreCase(zkMode)) {
			try {
				m_zkServer = new TestingServer(2181);
				System.out.println("Starting zk with fake mode, connection string is " + m_zkServer.getConnectString());
			} catch (Throwable e) {
				System.out.println("Zookeeper serer start failed, maybe started already.");
			}
		}
		super.startServer();
	}

	@Override
	protected void stopServer() throws Exception {
		super.stopServer();
		if (m_zkServer != null) {
			m_zkServer.stop();
		}
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return 7678;
	}

	@Override
	protected void postConfigure(WebAppContext context) {
		context.addFilter(GzipFilter.class, "/console/*", Handler.ALL);
	}

	@Test
	public void startWebapp() throws Exception {
		// open the page in the default browser
		display("/console");
		waitForAnyKey();
	}

}
