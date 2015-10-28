package com.ctrip.hermes.monitor.checker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.ProduceLatencyTooLargeEvent;
import com.ctrip.hermes.monitor.checker.client.ProduceLatencyChecker;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ProduceLatencyCheckerTest {
	public static class MockProduceLatencyChecker extends ProduceLatencyChecker {
		private String m_catReportXml;

		public MockProduceLatencyChecker(String catReportXml) {
			m_catReportXml = catReportXml;
		}

		@Override
		protected String getCatTransactionCrossReport(Date startHour, String transactionType) throws IOException {
			return m_catReportXml;
		}
	}

	@Test
	public void testAlert() throws Exception {
		String catReportXml = loadTestData("testAlert");
		ProduceLatencyChecker checker = new MockProduceLatencyChecker(catReportXml);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 50);

		CheckerResult result = checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertEquals(3, result.getMonitorEvents().size());

		List<MonitorEvent> expectedEvents = new ArrayList<>();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		calendar.set(Calendar.SECOND, 0);

		calendar.set(Calendar.MINUTE, 45);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("a.b.c", sdf.format(calendar.getTime()), 1000.1d));
		calendar.set(Calendar.MINUTE, 47);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("a.b.c", sdf.format(calendar.getTime()), 2000.3));
		calendar.set(Calendar.MINUTE, 46);
		expectedEvents.add(new ProduceLatencyTooLargeEvent("b.c.d", sdf.format(calendar.getTime()), 1329.6));

		for (MonitorEvent expectedEvent : expectedEvents) {
			assertTrue(result.getMonitorEvents().contains(expectedEvent));
		}
	}

	@Test
	public void testNormal() throws Exception {
		String catReportXml = loadTestData("testNormal");
		ProduceLatencyChecker checker = new MockProduceLatencyChecker(catReportXml);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 50);

		CheckerResult result = checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertTrue(result.getMonitorEvents().isEmpty());
	}

	private String loadTestData(String methodName) throws IOException, URISyntaxException {
		return new String(Files.readAllBytes(Paths.get(this.getClass()
		      .getResource(this.getClass().getSimpleName() + "-" + methodName + ".xml").toURI())));
	}
}
