package com.ctrip.hermes.broker;

import java.io.File;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.dianping.cat.Cat;

public class BrokerServer {
	public static void main(String[] args) throws Exception {
		File catConfigFile = new File("/data/appdatas/cat/client.xml");
		if (!catConfigFile.isFile() || !catConfigFile.canRead()) {
			throw new RuntimeException(String.format("Cat config file %s not found", catConfigFile.getCanonicalPath()));
		}

		Cat.initialize(catConfigFile);
		PlexusComponentLocator.lookup(BrokerBootstrap.class).start();
	}
}
