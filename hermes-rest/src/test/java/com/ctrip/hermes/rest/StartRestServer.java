package com.ctrip.hermes.rest;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.rest.HermesRestServer;
import com.ctrip.hermes.rest.service.MessagePushService;

public class StartRestServer extends ComponentTestCase {

	@Test
	public void start() throws Exception {
		HermesRestServer hermesRestServer = new HermesRestServer("com.ctrip.hermes.rest.resource");
		hermesRestServer.doStartup();

		lookup(MessagePushService.class).start();

		Thread.currentThread().join();
	}

}
