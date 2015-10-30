package com.ctrip.hermes.monitor.checker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.ProduceFailureCountTooLargeEvent;
import com.ctrip.hermes.monitor.checker.client.ProduceFailureChecker;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ProduceFailureCheckerTest extends BaseCheckerTest {
	@Component("MockProduceFailureChecker")
	public static class MockProduceFailureChecker extends ProduceFailureChecker {
		private String m_catReportXml;

		public void setCatReportXml(String catReportXml) {
			m_catReportXml = catReportXml;
		}

		@Override
		protected String curl(String url, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
			return m_catReportXml;
		}
	}

	@Autowired
	@Qualifier("MockProduceFailureChecker")
	private MockProduceFailureChecker m_checker;

	@Test
	public void testAlert() throws Exception {
		 String catReportXml = loadTestData("testAlert");
		 m_checker.setCatReportXml(catReportXml);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 36);

		CheckerResult result = m_checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertEquals(3, result.getMonitorEvents().size());

		List<MonitorEvent> expectedEvents = new ArrayList<>();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		calendar.set(Calendar.SECOND, 0);

		calendar.set(Calendar.MINUTE, 31);
		expectedEvents.add(new ProduceFailureCountTooLargeEvent("order_new", sdf.format(calendar.getTime()), 112));
		calendar.set(Calendar.MINUTE, 34);
		expectedEvents.add(new ProduceFailureCountTooLargeEvent("order_new", sdf.format(calendar.getTime()), 10));
		calendar.set(Calendar.MINUTE, 35);
		expectedEvents.add(new ProduceFailureCountTooLargeEvent("order_new", sdf.format(calendar.getTime()), 24));

		for (MonitorEvent expectedEvent : expectedEvents) {
			assertTrue(result.getMonitorEvents().contains(expectedEvent));
		}
	}

	@Test
	public void testNormal() throws Exception {
		String catReportXml = loadTestData("testNormal");
		m_checker.setCatReportXml(catReportXml);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 36);

		CheckerResult result = m_checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertTrue(result.getMonitorEvents().isEmpty());
	}

}
