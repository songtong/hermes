package com.ctrip.hermes.collector.notice.impl;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.hub.NotifierManager;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class PotentialIssueNoticeTest {
	@Autowired
	private NotifierManager m_notifierManager;
	
	@Test
	public void testGenerateNotice() throws IOException {
		PotentialIssueNotice potentialIssueNotice = new PotentialIssueNotice();
		potentialIssueNotice.setMessage("test");
		HermesNotice notice = new HermesNotice(Arrays.asList("lxteng@Ctrip.com"), potentialIssueNotice);		

		m_notifierManager.offer(notice, null);
		System.in.read();
	}

}
