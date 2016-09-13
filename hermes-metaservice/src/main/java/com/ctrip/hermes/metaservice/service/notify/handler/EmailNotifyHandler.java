package com.ctrip.hermes.metaservice.service.notify.handler;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.service.mail.HermesMail;
import com.ctrip.hermes.metaservice.service.mail.MailService;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailContext;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailUtil;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.TemplateService;
import com.google.common.util.concurrent.RateLimiter;

@Named(type = NotifyHandler.class, value = EmailNotifyHandler.ID)
public class EmailNotifyHandler implements NotifyHandler {
	private static final Logger log = LoggerFactory.getLogger(EmailNotifyHandler.class);

	public static final String ID = "EmailNotifyHandler";

	@Inject
	private TemplateService m_templateService;

	@Inject
	private MailService m_mailService;

	@Override
	public boolean handle(HermesNotice notice) {
		HermesMailContext mailCtx = HermesMailUtil.getHermesMailContext((MailNoticeContent) notice.getContent());
		String content = m_templateService.render(mailCtx.getHermesTemplate(), mailCtx.getContentMap());
		HermesMail mail = new HermesMail(mailCtx.getTitle(), content, notice.getReceivers());
		try {
			m_mailService.sendEmail(mail);
			log.info("Sent Email to: {}, Subject: {}", mail.getReceivers(), mail.getSubject());
			return true;
		} catch (Exception e) {
			log.error("Send Hermes mail failed: {}", mail, e);
		}
		return false;
	}
}
