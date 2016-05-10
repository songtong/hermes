package com.ctrip.hermes.collector.hub;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.notice.impl.PotentialIssueNotice;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.NotifyService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class NotifierManagerTest {
	@Autowired
	private NotifierManager m_manager;
	
	private NotifyService m_service = PlexusComponentLocator.lookup(NotifyService.class);
	
	@Test
	public void test() throws IOException, InterruptedException {
		m_service.setCategoryDefaultRate("default", 5, TimeUnit.SECONDS);
		for (int index = 0; index < 10; index++) {
			PotentialIssueNotice noticeContent = new PotentialIssueNotice();
			noticeContent.setMessage("test");
			m_manager.offer(new HermesNotice(Collections.singletonList("lxteng@Ctrip.com"), noticeContent), null);
			Thread.sleep(1000);
		}
		
		System.in.read();
	}

}
