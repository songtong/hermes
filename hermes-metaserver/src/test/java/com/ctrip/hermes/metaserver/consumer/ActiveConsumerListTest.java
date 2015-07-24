package com.ctrip.hermes.metaserver.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ActiveConsumerListTest {

	private ActiveConsumerList m_list;

	@Before
	public void before() throws Exception {
		m_list = new ActiveConsumerList();
	}

	@Test
	public void testHeartbeat() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		int port = 1234;
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip, port);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		assertClientContext(consumerName, ip, port, heartbeatTime, activeConsumers.get(consumerName));
		assertTrue(m_list.getAndResetChanged());
	}

	@Test
	public void testHeartbeatTwiceWithoutChange() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		int port = 1234;
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip, port);
		m_list.getAndResetChanged();
		m_list.heartbeat(consumerName, heartbeatTime + 1, ip, port);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		assertClientContext(consumerName, ip, port, heartbeatTime + 1, activeConsumers.get(consumerName));
		assertFalse(m_list.getAndResetChanged());

	}

	@Test
	public void testHeartbeatTwiceWithChange() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		int port = 1234;
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip, port);
		m_list.getAndResetChanged();
		m_list.heartbeat(consumerName, heartbeatTime + 1, ip + "2", port + 1);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		assertClientContext(consumerName, ip + "2", port + 1, heartbeatTime + 1, activeConsumers.get(consumerName));
		assertTrue(m_list.getAndResetChanged());

	}

	@Test
	public void testPurgeExpired() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		int port = 1234;
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip, port);
		m_list.purgeExpired(10, 12L);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(0, activeConsumers.size());
		assertTrue(m_list.getAndResetChanged());

		m_list.heartbeat(consumerName, heartbeatTime, ip, port);
		m_list.purgeExpired(10, 1L);
		activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		assertClientContext(consumerName, ip, port, heartbeatTime, activeConsumers.get(consumerName));
		assertTrue(m_list.getAndResetChanged());
	}

	private void assertClientContext(String consumerName, String ip, int port, long lastHeartbeatTime,
	      ClientContext clientContext) {
		assertEquals(consumerName, clientContext.getName());
		assertEquals(ip, clientContext.getIp());
		assertEquals(port, clientContext.getPort());
		assertEquals(lastHeartbeatTime, clientContext.getLastHeartbeatTime());
	}
}
