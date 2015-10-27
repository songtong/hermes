package com.ctrip.hermes.portal.service.monitor;

import org.junit.After;
import org.junit.Before;

import com.ctrip.hermes.portal.StartPortal;
import com.ctrip.hermes.portal.service.dashboard.DashboardService;

public class DefaultMonitorServiceTest extends StartPortal {
	private DashboardService m_monitor;

	@Before
	public void init() {
		m_monitor = lookup(DashboardService.class);
	}

	@After
	public void close() throws Exception {
		stopServer();
	}



}
