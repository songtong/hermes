package com.ctrip.hermes.admin.core.service.notify;

public class SmsNoticeContent extends ShortNoticeContent {

	public SmsNoticeContent(String content) {
		super(content);
	}

	@Override
	public HermesNoticeType getType() {
		return HermesNoticeType.SMS;
	}
}
