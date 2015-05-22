package com.ctrip.hermes.broker;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartBroker extends ComponentTestCase {

	@Test
	public void test() throws Exception {

		lookup(BrokerBootstrap.class).start();
		System.in.read();
	}
}
