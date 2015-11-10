package com.ctrip.hermes.monitor.checker;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.checker.server.BrokerLogErrorChecker;
import com.ctrip.hermes.monitor.checker.server.MetaserverLogErrorChecker;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ServerLogErrorCheckerTest {

	@Autowired
	private BrokerLogErrorChecker m_brokerChecker;

	@Autowired
	private MetaserverLogErrorChecker m_metaChecker;

	@Test
	public void testBrokerLogError() {
		CheckerResult result = m_brokerChecker.check(new Date(), (int) TimeUnit.DAYS.toMinutes(90));
		for (MonitorEvent e : result.getMonitorEvents()) {
			System.out.println(e);
		}
	}

}