package com.ctrip.hermes.monitor.checker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.xml.sax.SAXException;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.ProduceAckedTriedRatioErrorEvent;
import com.ctrip.hermes.monitor.checker.client.ProduceAckedTriedRatioChecker;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ProduceAckedTriedRatioCheckerTest extends BaseCheckerTest {
	@Component("MockProduceAckedTriedRatioChecker")
	public static class MockProduceAckedTriedRatioChecker extends ProduceAckedTriedRatioChecker {
		private String m_catTriedReportXml;

		private String m_catAckedReportXml;

		private Meta m_meta;

		public void setCatTriedReportXml(String catTriedReportXml) {
			m_catTriedReportXml = catTriedReportXml;
		}

		public void setCatAckedReportXml(String catAckedReportXml) {
			m_catAckedReportXml = catAckedReportXml;
		}

		public void setMeta(Meta meta) {
			m_meta = meta;
		}

		@Override
		protected Meta fetchMeta() throws IOException, SAXException {
			return m_meta;
		}

		@Override
		protected String getTransactionReportFromCat(Timespan timespan, String transactionType) throws IOException {
			if ("Message.Produce.Acked".equals(transactionType)) {
				return m_catAckedReportXml;
			} else if ("Message.Produce.Tried".equals(transactionType)) {
				return m_catTriedReportXml;
			}

			return null;
		}
	}

	@Autowired
	@Qualifier("MockProduceAckedTriedRatioChecker")
	private MockProduceAckedTriedRatioChecker m_checker;

	@Test
	public void testAlert() throws Exception {
		String catTriedReportXml = loadTestData("testAlert-tried");
		String catAckedReportXml = loadTestData("testAlert-acked");
		m_checker.setCatTriedReportXml(catTriedReportXml);
		m_checker.setCatAckedReportXml(catAckedReportXml);

		Meta meta = DefaultSaxParser.parse(loadTestData("testAlert-meta"));
		m_checker.setMeta(meta);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 9);
		calendar.set(Calendar.DAY_OF_MONTH, 10);
		calendar.set(Calendar.HOUR_OF_DAY, 10);
		calendar.set(Calendar.MINUTE, 5);
		calendar.set(Calendar.SECOND, 5);

		CheckerResult result = m_checker.check(calendar.getTime(), 5);

		assertTrue(result.isRunSuccess());
		assertNull(result.getErrorMessage());
		assertNull(result.getException());
		assertEquals(2, result.getMonitorEvents().size());

		List<MonitorEvent> expectedEvents = new ArrayList<>();

		expectedEvents.add(new ProduceAckedTriedRatioErrorEvent("c.d.e", "2015-10-10 10:00:00 ~ 2015-10-10 10:04:59", 10,
		      2));
		expectedEvents.add(new ProduceAckedTriedRatioErrorEvent("d.e.f", "2015-10-10 10:00:00 ~ 2015-10-10 10:04:59", 0,
		      130));

		for (MonitorEvent expectedEvent : expectedEvents) {
			assertTrue(result.getMonitorEvents().contains(expectedEvent));
		}
	}

	// @Test
	// public void testNormal() throws Exception {
	// String catReportXml = loadTestData("testNormal");
	// m_checker.setCatReportXml(catReportXml);
	//
	// Calendar calendar = Calendar.getInstance();
	// calendar.set(Calendar.MINUTE, 50);
	//
	// CheckerResult result = m_checker.check(calendar.getTime(), 5);
	//
	// assertTrue(result.isRunSuccess());
	// assertNull(result.getErrorMessage());
	// assertNull(result.getException());
	// assertTrue(result.getMonitorEvents().isEmpty());
	// }

}
