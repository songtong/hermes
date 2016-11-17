package com.ctrip.hermes.monitor.checker;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.checker.server.BrokerLogErrorChecker;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ServerLogErrorCheckerTest {

	@Autowired
	private BrokerLogErrorChecker m_brokerChecker;


	@Test
	public void testBrokerLogError() {
		CheckerResult result = m_brokerChecker.check(new Date(), (int) TimeUnit.DAYS.toMinutes(90));
		for (MonitorEvent e : result.getMonitorEvents()) {
			System.out.println(e);
		}
	}

}
