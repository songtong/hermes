package com.ctrip.hermes.monitor.checker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unidal.dal.jdbc.Readset;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessage;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.monitor.checker.mysql.ConsumeLargeBacklogChecker;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ConsumeLargeBacklogCheckerTest extends BaseCheckerTest {

	@Component("MockConsumeLargeBacklogChecker")
	public static class MockConsumeLargeBacklogChecker extends ConsumeLargeBacklogChecker {

		@Override
		protected Meta fetchMeta() {
			try {
				return DefaultSaxParser.parse(new String(Files.readAllBytes(Paths.get(this.getClass()
				      .getResource("DBCheckerMockMeta.xml").toURI()))));
			} catch (Exception e) {
				return null;
			}
		}

		@SuppressWarnings("unchecked")
		public void initMock(long msgId, long offset) {
			try {
				MessagePriorityDao mockMessageDao = mock(MessagePriorityDao.class);
				ArrayList<MessagePriority> msg = new ArrayList<MessagePriority>();
				msg.add(new MessagePriority().setId(msgId));

				ArrayList<MessagePriority> empty = new ArrayList<MessagePriority>();
				empty.add(new MessagePriority().setId(100));

				doReturn(empty)//
				      .when(mockMessageDao) //
				      .topK(anyString(), anyInt(), anyInt(), anyInt(), any(Readset.class));
				doReturn(msg)//
				      .when(mockMessageDao) //
				      .topK(eq("song.test"), eq(0), eq(0), anyInt(), any(Readset.class));
				setMessagePriorityDao(mockMessageDao);

				OffsetMessageDao mockOffsetDao = mock(OffsetMessageDao.class);
				doReturn(new OffsetMessage().setOffset(offset))//
				      .when(mockOffsetDao) //
				      .find(anyString(), anyInt(), anyInt(), anyInt(), any(Readset.class));
				doReturn(new OffsetMessage().setOffset(offset))//
				      .when(mockOffsetDao) //
				      .find(eq("song.test"), eq(0), eq(0), anyInt(), any(Readset.class));
				setOffsetMessageDao(mockOffsetDao);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Autowired
	@Qualifier("MockConsumeLargeBacklogChecker")
	private MockConsumeLargeBacklogChecker m_checker;

	@Test
	public void testNormal() throws Exception {
		m_checker.initMock(10000, 1000);
		CheckerResult result = m_checker.check(new Date(), (int) TimeUnit.DAYS.toMinutes(90));
		for (MonitorEvent e : result.getMonitorEvents()) {
			System.out.println(e);
		}
	}
}
