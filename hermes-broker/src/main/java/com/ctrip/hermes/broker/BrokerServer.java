package com.ctrip.hermes.broker;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.dianping.cat.Cat;

public class BrokerServer {
	public static void main(String[] args) throws Exception {
		// TODO System.setProperty("devMode", "true");
		Cat.initializeByDomain("900777", 2280, 80, "cat.fws.qa.nt.ctripcorp.com");

		PlexusComponentLocator.lookup(BrokerBootstrap.class).start();
	}
}
