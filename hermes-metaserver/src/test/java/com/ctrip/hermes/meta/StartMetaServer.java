package com.ctrip.hermes.meta;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.server.MetaRestServer;

public class StartMetaServer extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		lookup(MetaRestServer.class).start();
	}

}
