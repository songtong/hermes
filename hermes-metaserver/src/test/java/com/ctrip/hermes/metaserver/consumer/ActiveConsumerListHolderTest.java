package com.ctrip.hermes.metaserver.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.TestHelper;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ActiveConsumerListHolderTest {
	@Mock
	private SystemClockService m_systemClockService;

	private ActiveConsumerListHolder m_holder;

	@Before
	public void before() throws Exception {
		when(m_systemClockService.now()).thenReturn(1L);
		m_holder = new ActiveConsumerListHolder();
		m_holder.setSystemClockService(m_systemClockService);
	}

	@Test
	public void testHeartbeat() throws Exception {
		Pair<String, String> t1g1 = new Pair<>("t1", "g1");
		Pair<String, String> t1g2 = new Pair<>("t1", "g2");
		Pair<String, String> t2g1 = new Pair<>("t2", "g1");
		m_holder.heartbeat(t1g1, "c1", "1.1.1.1", 1111);
		m_holder.heartbeat(t1g1, "c2", "1.1.1.2", 2222);
		m_holder.heartbeat(t1g2, "c3", "1.1.1.3", 3333);
		m_holder.heartbeat(t2g1, "c1", "1.1.1.1", 1111);

		ActiveConsumerList activeConsumerList = m_holder.getActiveConsumerList(t1g1);
		Map<String, ClientContext> activeConsumers = activeConsumerList.getActiveConsumers();
		assertEquals(2, activeConsumers.size());
		TestHelper.assertClientContextEquals("c1", "1.1.1.1", 1111, activeConsumers.get("c1"));
		TestHelper.assertClientContextEquals("c2", "1.1.1.2", 2222, activeConsumers.get("c2"));

		activeConsumerList = m_holder.getActiveConsumerList(t1g2);
		activeConsumers = activeConsumerList.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals("c3", "1.1.1.3", 3333, activeConsumers.get("c3"));

		activeConsumerList = m_holder.getActiveConsumerList(t2g1);
		activeConsumers = activeConsumerList.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals("c1", "1.1.1.1", 1111, activeConsumers.get("c1"));
	}

	@Test
	public void test() throws Exception {
		Pair<String, String> t1g1 = new Pair<>("t1", "g1");
		Pair<String, String> t1g2 = new Pair<>("t1", "g2");
		Pair<String, String> t2g1 = new Pair<>("t2", "g1");
		Pair<String, String> t3g1 = new Pair<>("t3", "g1");

		m_holder.heartbeat(t1g1, "c5", "1.1.1.5", 5555);
		when(m_systemClockService.now()).thenReturn(100L);
		m_holder.heartbeat(t3g1, "c15", "1.1.1.15", 8888);
		when(m_systemClockService.now()).thenReturn(1L);

		Map<Pair<String, String>, Map<String, ClientContext>> changes = m_holder.scanChanges(1, TimeUnit.MILLISECONDS);

		assertEquals(2, changes.size());

		Map<String, ClientContext> activeConsumers = changes.get(t1g1);
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals("c5", "1.1.1.5", 5555, activeConsumers.get("c5"));
		activeConsumers = changes.get(t3g1);
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals("c15", "1.1.1.15", 8888, activeConsumers.get("c15"));

		when(m_systemClockService.now()).thenReturn(1L);
		m_holder.heartbeat(t1g1, "c1", "1.1.1.1", 1111);

		when(m_systemClockService.now()).thenReturn(5L);
		m_holder.heartbeat(t1g1, "c2", "1.1.1.2", 2222);

		when(m_systemClockService.now()).thenReturn(3L);
		m_holder.heartbeat(t1g2, "c3", "1.1.1.3", 3333);

		when(m_systemClockService.now()).thenReturn(7L);
		m_holder.heartbeat(t2g1, "c1", "1.1.1.1", 1111);

		when(m_systemClockService.now()).thenReturn(5L);
		changes = m_holder.scanChanges(1, TimeUnit.MILLISECONDS);

		assertEquals(3, changes.size());

		activeConsumers = changes.get(t1g1);
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals("c2", "1.1.1.2", 2222, activeConsumers.get("c2"));

		activeConsumers = changes.get(t1g2);
		assertTrue(activeConsumers.isEmpty());

		activeConsumers = changes.get(t2g1);
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals("c1", "1.1.1.1", 1111, activeConsumers.get("c1"));
	}
}
