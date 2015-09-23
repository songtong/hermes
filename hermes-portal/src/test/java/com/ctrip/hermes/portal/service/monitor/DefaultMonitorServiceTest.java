package com.ctrip.hermes.portal.service.monitor;

import org.junit.After;
import org.junit.Before;

import com.ctrip.hermes.portal.StartPortal;

public class DefaultMonitorServiceTest extends StartPortal {
	private MonitorService m_monitor;

	@Before
	public void init() {
		m_monitor = lookup(MonitorService.class);
	}

	@After
	public void close() throws Exception {
		stopServer();
	}



}
