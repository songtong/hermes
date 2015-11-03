package com.ctrip.hermes.monitor.checker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Readset;

import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.queue.DeadLetter;
import com.ctrip.hermes.metaservice.queue.DeadLetterDao;
import com.ctrip.hermes.monitor.checker.mysql.TopicLargeDeadLetterChecker;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TopicLargeDeadLetterCheckerTest.class)
public class TopicLargeDeadLetterCheckerTest extends BaseCheckerTest {

	@Component("MockTopicLargeDeadLetterChecker")
	public static class MockTopicLargeDeadLetterChecker extends TopicLargeDeadLetterChecker {
		@SuppressWarnings("unchecked")
		public void initMock(int count) {
			try {
				setLimits(parseLimits(
				      DefaultSaxParser.parse(loadTestData("testNormal", TopicLargeDeadLetterCheckerTest.class)),
				      m_config.getDeadLetterCheckerIncludeTopics(), m_config.getDeadLetterCheckerExcludeTopics()));
				DeadLetterDao mockDao = mock(DeadLetterDao.class);
				try {
					doReturn(new DeadLetter().setCountOfTimeRange(100)) //
					      .when(mockDao) //
					      .findByTimeRange(anyString(), anyInt(), any(Date.class), any(Date.class), any(Readset.class));
					doReturn(new DeadLetter().setCountOfTimeRange(count)) //
					      .when(mockDao) //
					      .findByTimeRange(eq("song.test"), anyInt(), any(Date.class), any(Date.class), any(Readset.class));
				} catch (DalException e) {
					e.printStackTrace();
				}
				setDeadLetterDao(mockDao);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Autowired
	@Qualifier("MockTopicLargeDeadLetterChecker")
	private MockTopicLargeDeadLetterChecker m_checker;

	@Test
	public void testNormal() throws Exception {
		m_checker.initMock(10);
		CheckerResult result = m_checker.check(new Date(), (int) TimeUnit.DAYS.toMinutes(90));
		for (MonitorEvent e : result.getMonitorEvents()) {
			System.out.println(e);
		}
	}
}
