package com.ctrip.hermes.admin.core.service.notify;

public class TtsNoticeContent extends ShortNoticeContent {

	public TtsNoticeContent(String content) {
		super(content);
	}

	@Override
	public HermesNoticeType getType() {
		return HermesNoticeType.TTS;
	}

}
