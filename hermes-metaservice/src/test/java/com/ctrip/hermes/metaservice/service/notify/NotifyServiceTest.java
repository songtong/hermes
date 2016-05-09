package com.ctrip.hermes.metaservice.service.notify;

import java.util.Arrays;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

public class NotifyServiceTest extends ComponentTestCase {

	@HermesMailDescription(template = HermesTemplate.TEST)
	public static class MMail extends MailNoticeContent {
		@ContentField(name = "name")
		private String m_name;

		@ContentField(name = "age")
		private int m_age;

		private String m_subject;

		public MMail(String sub, String name, int age) {
			m_subject = sub;
			m_name = name;
			m_age = age;
		}

		@Override
		public String getSubject() {
			return m_subject;
		}

		public String getName() {
			return m_name;
		}

		public int getAge() {
			return m_age;
		}
	}

	@Test
	public void testNotifyInMail() {
		HermesNotice n = new HermesNotice(Arrays.asList("xxx@xxx.xx"), new MMail("Hello Song", "song", 28));
		NotifyService s = lookup(NotifyService.class);
		s.notify(n);
	}
}
