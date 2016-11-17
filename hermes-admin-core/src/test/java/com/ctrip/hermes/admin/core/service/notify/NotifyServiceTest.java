package com.ctrip.hermes.admin.core.service.notify;

import java.util.Arrays;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.MailNoticeContent;
import com.ctrip.hermes.admin.core.service.notify.NotifyService;
import com.ctrip.hermes.admin.core.service.notify.SmsNoticeContent;
import com.ctrip.hermes.admin.core.service.template.HermesTemplate;

public class NotifyServiceTest extends ComponentTestCase {

	@HermesMailDescription(template = HermesTemplate.TEST)
	public static class MMail extends MailNoticeContent {
		@ContentField(name = "name")
		private String m_name;

		@ContentField(name = "age")
		private int m_age;

		@Subject
		private String m_subject;

		public MMail(String sub, String name, int age) {
			m_subject = sub;
			m_name = name;
			m_age = age;
		}

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
	public void testNotifyInMail() throws Exception {
		HermesNotice n = new HermesNotice(Arrays.asList("xxx@xxx.xx"), new MMail("Hello Song", "song", 28));
		NotifyService s = lookup(NotifyService.class);
		s.notify(n);
	}

	@Test
	public void testNotifyInSms() throws Exception {
		HermesNotice n = new HermesNotice(Arrays.asList("10101010101"), new SmsNoticeContent("Hello world"));
		NotifyService s = lookup(NotifyService.class);
		s.notify(n);
	}
}
