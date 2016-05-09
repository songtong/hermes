package com.ctrip.hermes.metaservice.service.notify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmsNoticeContent implements NoticeContent {
	private static final Logger log = LoggerFactory.getLogger(SmsNoticeContent.class);

	private static final int MAX_SMS_LEN = 280;

	private static final String SMS_OMIT = " ... ";

	private String m_content;

	public SmsNoticeContent(String content) {
		if (content.length() > MAX_SMS_LEN) {
			log.warn("Sms content \\{{}\\} too long!", content);
		}
		m_content = toShort(content, MAX_SMS_LEN);
	}

	private static String toShort(String str, int len) {
		int surplus = str.length() - len;
		if (surplus > SMS_OMIT.length()) {
			str = str.substring(0, len / 2) + SMS_OMIT + str.substring(str.length() - len / 2);
		}
		return str;
	}

	@Override
	public NoticeType getType() {
		return NoticeType.SMS;
	}

	public String getContent() {
		return m_content;
	}

	@Override
	public String toString() {
		return "SmsNoticeContent [m_content=" + m_content + ", getType()=" + getType() + "]";
	}
}
