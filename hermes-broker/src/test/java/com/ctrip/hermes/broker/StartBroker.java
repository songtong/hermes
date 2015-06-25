package com.ctrip.hermes.broker;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartBroker extends ComponentTestCase {
	private TestingServer m_zkServer;

	@Before
	public void before() {
		String zkMode = System.getProperty("zkMode");
		if (!"real".equalsIgnoreCase(zkMode)) {
			try {
				m_zkServer = new TestingServer(2181);
			} catch (Exception e) {
				System.out.println("Start zk fake server failed, may be already started.");
			}
		}
	}

	@After
	public void after() throws Exception {
		if (m_zkServer != null) {
			m_zkServer.stop();
		}
	}

	@Test
	public void test() throws Exception {

		lookup(BrokerBootstrap.class).start();
		System.in.read();
	}
}
