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
import com.ctrip.hermes.metaservice.monitor.event.ProduceTransportFailedRatioErrorEvent;
import com.ctrip.hermes.monitor.checker.client.ProduceTransportFailedRatioChecker;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ProduceTransportFailedRatioCheckerTest extends BaseCheckerTest {
	@Component("MockProduceTransportFailedRatioChecker")
	public static class MockProduceTransportFailedRatioChecker extends ProduceTransportFailedRatioChecker {
		private String m_catTransportReportXml;

		private Meta m_meta;

		public void setTransportCmdReportXml(String catTransportReportXml) {
			m_catTransportReportXml = catTransportReportXml;
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
			if ("Message.Produce.Transport".equals(transactionType)) {
				return m_catTransportReportXml;
			}

			return null;
		}
	}

	@Autowired
	@Qualifier("MockProduceTransportFailedRatioChecker")
	private MockProduceTransportFailedRatioChecker m_checker;

	@Test
	public void testAlert() throws Exception {
		String catTransportReportXml = loadTestData("testAlert-transport");
		m_checker.setTransportCmdReportXml(catTransportReportXml);

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

		expectedEvents.add(new ProduceTransportFailedRatioErrorEvent("c.d.e", "2015-10-10 10:00:00 ~ 2015-10-10 10:04:59",
		      10, 2));
		expectedEvents.add(new ProduceTransportFailedRatioErrorEvent("d.e.f", "2015-10-10 10:00:00 ~ 2015-10-10 10:04:59",
		      0, 130));

		for (MonitorEvent expectedEvent : expectedEvents) {
			assertTrue(result.getMonitorEvents().contains(expectedEvent));
		}
	}
}
