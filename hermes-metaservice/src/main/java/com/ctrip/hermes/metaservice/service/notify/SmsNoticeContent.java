package com.ctrip.hermes.metaservice.service.notify;

public class SmsNoticeContent extends ShortNoticeContent {

	public SmsNoticeContent(String content) {
		super(content);
	}

	@Override
	public NoticeType getType() {
		return NoticeType.SMS;
	}
}
