package com.ctrip.hermes.metaservice.service.notify;

public class TtsNoticeContent extends ShortNoticeContent {

	public TtsNoticeContent(String content) {
		super(content);
	}

	@Override
	public HermesNoticeType getType() {
		return HermesNoticeType.TTS;
	}

}
