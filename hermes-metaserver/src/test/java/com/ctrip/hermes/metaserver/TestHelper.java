package com.ctrip.hermes.metaserver;

import static org.junit.Assert.assertEquals;

import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class TestHelper {
	public static void assertClientContextEquals(String clientName, String ip, int port, long lastHeartbeatTime,
	      ClientContext clientContext) {
		assertEquals(clientName, clientContext.getName());
		assertEquals(ip, clientContext.getIp());
		assertEquals(port, clientContext.getPort());
		assertEquals(lastHeartbeatTime, clientContext.getLastHeartbeatTime());
	}

	public static void assertClientContextEquals(String clientName, String ip, int port, ClientContext clientContext) {
		assertEquals(clientName, clientContext.getName());
		assertEquals(ip, clientContext.getIp());
		assertEquals(port, clientContext.getPort());
	}

}
