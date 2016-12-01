package com.ctrip.hermes.collector.notice.impl;

import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.admin.core.service.notify.MailNoticeContent;
import com.ctrip.hermes.admin.core.service.template.HermesTemplate;

@HermesMailDescription(template = HermesTemplate.POTENTIAL_ISSUE_NOTICE)
public class PotentialIssueNotice extends MailNoticeContent {
	@Subject
	private String m_subject = "Potential Issue";

	@ContentField(name = "message")
	private String m_message;

	public String getSubject() {
		return m_subject;
	}

	public void setSubject(String subject) {
		m_subject = subject;
	}

	public String getMessage() {
		return m_message;
	}

	public void setMessage(String message) {
		m_message = message;
	}
}
