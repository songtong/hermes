package com.ctrip.hermes.metaservice.monitor.dao;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;

public class DefaultMonitorEventStorageTest extends ComponentTestCase {

	private MonitorEventStorage m_storage = PlexusComponentLocator.lookup(MonitorEventStorage.class);

	@Test
	public void testAddMonitorEvent() {
		BrokerErrorEvent e = new BrokerErrorEvent();
		try {
			e.setShouldNotify(true);
			m_storage.addMonitorEvent(e);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	@Test
	public void testAddAndCheckMonitorEvent() throws Exception {
		for (int i = 0; i < 10; i++) {
			m_storage.addMonitorEvent(new BrokerErrorEvent("127.0.0.1", 10));
		}
		List<MonitorEvent> l = m_storage.fetchUnnotifiedMonitorEvent(false);
		for (MonitorEvent e : l) {
			System.out.println(e);
		}
		l = m_storage.fetchUnnotifiedMonitorEvent(true);
		for (MonitorEvent e : l) {
			System.out.println(e);
		}
		l = m_storage.fetchUnnotifiedMonitorEvent(false);
		Assert.assertEquals(0, l.size());
		l = m_storage.fetchUnnotifiedMonitorEvent(true);
		Assert.assertEquals(0, l.size());
	}
}
