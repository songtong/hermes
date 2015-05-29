package com.ctrip.hermes.rest;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.rest.service.MessagePushService;

public class StartRestServer extends ComponentTestCase {

	@Test
	public void start() throws Exception {
		HermesRestServer hermesRestServer = new HermesRestServer();
		hermesRestServer.start();

		lookup(MessagePushService.class).start();

		Thread.currentThread().join();
	}

}
