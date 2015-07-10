package com.ctrip.hermes.portal.service.monitor;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

	@Test
	public void testGetDelay() throws Exception {
		for (int i = 0; i < 5; i++) {
			System.out.println(m_monitor.getDelay("order_new", 1));
			TimeUnit.SECONDS.sleep(1);
		}
	}

}
