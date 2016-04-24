package com.ctrip.hermes.mail;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.service.mail.HermesMail;
import com.ctrip.hermes.metaservice.service.mail.MailService;

public class DefaultMailServiceTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		MailService ms = lookup(MailService.class);
		String subject = "Hello from Herms";
		String body = "<html><body><b>HERMES</b></body><html>";
		String receivers = "q_gu@ctrip.com,jhliang@ctrip.com,qingyang@Ctrip.com";
		HermesMail mail = new HermesMail(subject, body, receivers);
		ms.sendEmail(mail);
	}

}
