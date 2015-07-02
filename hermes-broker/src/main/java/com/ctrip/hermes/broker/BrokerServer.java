package com.ctrip.hermes.broker;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.broker.shutdown.ShutdownRequestMonitor;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.dianping.cat.Cat;

public class BrokerServer {
	private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

	public static void main(String[] args) throws Exception {


		File catConfigFile = new File("/opt/ctrip/data/cat/client.xml");
		if (!catConfigFile.isFile() || !catConfigFile.canRead()) {
			throw new IllegalStateException(
					  String.format("Cat config file %s not found", catConfigFile.getCanonicalPath()));
		}

		Cat.initialize(catConfigFile);
		PlexusComponentLocator.lookup(BrokerBootstrap.class).start();

		Runtime.getRuntime().addShutdownHook(
				  new Thread() {
					  @Override
					  public void run() {
						  log.warn("==============================\n" +
									 "Going to shutdown. (\"shutdown task xxxx\" is from CAT)\n");
						  PlexusComponentLocator.lookup(ShutdownRequestMonitor.class).stopBroker();
						  log.warn("Shutdown is done.\n==========================================");

					  }
				  }
		);
	}
}
