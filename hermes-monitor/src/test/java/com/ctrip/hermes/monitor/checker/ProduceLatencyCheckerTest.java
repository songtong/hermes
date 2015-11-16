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
import com.ctrip.hermes.metaservice.monitor.event.ProduceLatencyTooLargeEvent;
import com.ctrip.hermes.monitor.checker.client.ProduceLatencyChecker;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ProduceLatencyCheckerTest extends BaseCheckerTest {
	@Component("MockProduceLatencyChecker")
	public static class MockProduceLatencyChecker extends ProduceLatencyChecker {
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
	@Qualifier("MockProduceLatencyChecker")
	private MockProduceLatencyChecker m_checker;

	@Test
	public void testAlert() throws Exception {
		String catReportXml = loadTestData("testAlert");
		m_checker.setCatReportXml(catReportXml);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 50);

		CheckerResult result = m_checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertEquals(4, result.getMonitorEvents().size());

		List<MonitorEvent> expectedEvents = new ArrayList<>();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		calendar.set(Calendar.SECOND, 0);

		calendar.set(Calendar.MINUTE, 45);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("a.b.c", sdf.format(calendar.getTime()), 1000.1d));
		calendar.set(Calendar.MINUTE, 47);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("a.b.c", sdf.format(calendar.getTime()), 2000.3));
		calendar.set(Calendar.MINUTE, 46);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("b.c.d", sdf.format(calendar.getTime()), 1329.6));
		calendar.set(Calendar.MINUTE, 49);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("f.g.h", sdf.format(calendar.getTime()), 6329.6));

		for (MonitorEvent expectedEvent : expectedEvents) {
			assertTrue(result.getMonitorEvents().contains(expectedEvent));
		}
	}

	@Test
	public void testNormal() throws Exception {
		String catReportXml = loadTestData("testNormal");
		m_checker.setCatReportXml(catReportXml);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 50);

		CheckerResult result = m_checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertTrue(result.getMonitorEvents().isEmpty());
	}

}
