package com.ctrip.hermes.metaservice.service.notify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ShortNoticeContent implements NoticeContent {
	private static final Logger log = LoggerFactory.getLogger(ShortNoticeContent.class);

	private static final int MAX_CONTENT_LEN = 280;

	private static final String CONTENT_OMIT = " ... ";

	private String m_content;

	public ShortNoticeContent(String content) {
		if (content.length() > MAX_CONTENT_LEN) {
			log.warn("Notice content {{}} too long!", content);
		}
		m_content = toShort(content, MAX_CONTENT_LEN);
	}

	private String toShort(String str, int len) {
		int surplus = str.length() - len;
		if (surplus > CONTENT_OMIT.length()) {
			str = str.substring(0, len / 2) + CONTENT_OMIT + str.substring(str.length() - len / 2);
		}
		return str;
	}

	public String getContent() {
		return m_content;
	}

	@Override
	public String toString() {
		return "ShortNoticeContent [m_content=" + m_content + ", getType()=" + getType() + "]";
	}
}
