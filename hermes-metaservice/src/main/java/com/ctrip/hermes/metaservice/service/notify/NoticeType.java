package com.ctrip.hermes.metaservice.service.notify;

import com.ctrip.hermes.metaservice.service.notify.handler.EmailNotifyHandler;
import com.ctrip.hermes.metaservice.service.notify.handler.SmsNotifyHandler;
import com.ctrip.hermes.metaservice.service.notify.handler.TtsNotifyHandler;

public enum NoticeType {
	
	TTS(TtsNotifyHandler.ID), //

	SMS(SmsNotifyHandler.ID), //

	EMAIL(EmailNotifyHandler.ID);

	private String m_handlerId;

	private NoticeType(String handlerId) {
		m_handlerId = handlerId;
	}

	public String handlerID() {
		return m_handlerId;
	}
}
