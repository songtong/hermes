package com.ctrip.hermes.monitor.checker;

import java.io.IOException;
import java.util.Calendar;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.checker.meta.DefaultMetaRequestErrorChecker;

public class MetaRequestErrorCheckerTest extends BaseCheckerTest {
	public static class MockMetaRequestErrorChecker extends DefaultMetaRequestErrorChecker {
		private String m_xmlContent;

		public void setCatXml(String catXml) {
			m_xmlContent = catXml;
		}

		@Override
		protected String getCatCrossDomainTransactionReport(Timespan timespan, String transactionType) throws IOException {
			return m_xmlContent;
		}

	}

	@Autowired
	@Qualifier("MockMetaRequestErrorChecker")
	private MockMetaRequestErrorChecker m_checker;

	@Test
	public void testMetaError() throws Exception {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 9);
		calendar.set(Calendar.DAY_OF_MONTH, 10);
		calendar.set(Calendar.HOUR_OF_DAY, 10);
		calendar.set(Calendar.MINUTE, 5);
		calendar.set(Calendar.SECOND, 5);

		m_checker.setCatXml(loadTestData("testMetaError"));
		CheckerResult result = m_checker.check(calendar.getTime(), 5);
		for (MonitorEvent event : result.getMonitorEvents()) {
			System.out.println(event);
		}

	}

}
