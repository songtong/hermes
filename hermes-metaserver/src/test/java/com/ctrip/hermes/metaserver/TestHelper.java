package com.ctrip.hermes.metaserver;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
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

	public static Meta loadLocalMeta(Object testCase) throws Exception {
		String fileName = testCase.getClass().getSimpleName() + "-meta.xml";
		InputStream in = testCase.getClass().getResourceAsStream(fileName);

		if (in == null) {
			throw new RuntimeException(String.format("File %s not found in classpath.", fileName));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (Exception e) {
				throw new RuntimeException(String.format("Error parse meta file %s", fileName), e);
			}
		}
	}

}
