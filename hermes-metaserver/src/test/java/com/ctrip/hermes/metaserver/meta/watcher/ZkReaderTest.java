package com.ctrip.hermes.metaserver.meta.watcher;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

public class ZkReaderTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		ZkReader zkReader = lookup(ZkReader.class);
		zkReader.readPartition2Endpoint("order_new");
	}

}
