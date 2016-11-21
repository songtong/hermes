package com.ctrip.hermes.monitor.checker;

import java.io.IOException;
import java.util.Calendar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.checker.client.ConsumerAckErrorChecker;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ConsumerAckErrorCheckerTest extends BaseCheckerTest {

	@Component(value = "MockConsumerAckErrorChecker")
	public static class MockConsumerAckErrorChecker extends ConsumerAckErrorChecker {
		private String m_xmlContent;

		public void setCatXml(String xmlContent) {
			m_xmlContent = xmlContent;
		}

		@Override
		protected String getCatCrossDomainTransactionReport(Timespan timespan, String transactionType) throws IOException {
			return m_xmlContent;
		}
	}

	@Autowired
	@Qualifier("MockConsumerAckErrorChecker")
	MockConsumerAckErrorChecker m_checker;

	@Test
	public void testAckError() throws Exception {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 9);
		calendar.set(Calendar.DAY_OF_MONTH, 10);
		calendar.set(Calendar.HOUR_OF_DAY, 10);
		calendar.set(Calendar.MINUTE, 5);
		calendar.set(Calendar.SECOND, 5);

		m_checker.setCatXml(loadTestData("testAckError"));
		CheckerResult result = m_checker.check(calendar.getTime(), 5);
		for (MonitorEvent event : result.getMonitorEvents()) {
			System.out.println(event);
		}
	}
}
