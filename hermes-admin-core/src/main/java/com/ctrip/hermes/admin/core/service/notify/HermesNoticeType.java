package com.ctrip.hermes.admin.core.service.notify;

import com.ctrip.hermes.admin.core.service.notify.handler.EmailNotifyHandler;
import com.ctrip.hermes.admin.core.service.notify.handler.SMSNotifyHandler;
import com.ctrip.hermes.admin.core.service.notify.handler.TTSNotifyHandler;

public enum HermesNoticeType {

	TTS(TTSNotifyHandler.ID, 60), //

	SMS(SMSNotifyHandler.ID, 30), //

	EMAIL(EmailNotifyHandler.ID, 15);

	private String m_handlerId;

	private int m_defaultRateInMinute;

	private HermesNoticeType(String handlerId, int defaultRateInMinute) {
		m_handlerId = handlerId;
		m_defaultRateInMinute = defaultRateInMinute;
	}

	public String handlerID() {
		return m_handlerId;
	}

	public int defaultRateInMinute() {
		return m_defaultRateInMinute;
	}
}
