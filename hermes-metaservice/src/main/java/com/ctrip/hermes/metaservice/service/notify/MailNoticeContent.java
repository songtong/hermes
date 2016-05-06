package com.ctrip.hermes.metaservice.service.notify;

public abstract class MailNoticeContent implements NoticeContent {

	@Override
	public NoticeType getType() {
		return NoticeType.EMAIL;
	}

	public abstract String getSubject();

}
