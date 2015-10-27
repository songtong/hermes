package com.ctrip.hermes.metaservice.monitor.dao;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.monitor.event.BrokerErrorEvent;

public class DefaultMonitorEventStorageTest extends ComponentTestCase {

	private MonitorEventStorage m_storage = PlexusComponentLocator.lookup(MonitorEventStorage.class);

	@Test
	public void testAddMonitorEvent() {
		BrokerErrorEvent e = new BrokerErrorEvent();
		try {
			m_storage.addMonitorEvent(e);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
