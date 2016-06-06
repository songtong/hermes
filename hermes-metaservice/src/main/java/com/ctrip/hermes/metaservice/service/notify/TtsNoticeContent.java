package com.ctrip.hermes.metaservice.service.notify;

public class TtsNoticeContent extends ShortNoticeContent {

	public TtsNoticeContent(String content) {
		super(content);
	}

	@Override
	public NoticeType getType() {
		return NoticeType.TTS;
	}

}
