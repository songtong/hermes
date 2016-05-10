package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.eventhandler.ConsumerLargeBacklogEventHandler.ConsumerLargeBacklogEventReportMailNoticeContent;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.NotifyService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class TestMail {
	private NotifyService m_service = PlexusComponentLocator.lookup(NotifyService.class);
	
	@Test
	public void test() {
		ConsumerLargeBacklogEventReportMailNoticeContent content = new ConsumerLargeBacklogEventReportMailNoticeContent();
		m_service.notify(new HermesNotice(Arrays.asList("lxteng@ctrip.com"), content), "");
	}
}
