package com.ctrip.hermes.core.result;

public class SendResult {

	private boolean m_success = false;

	public SendResult(boolean success) {
		m_success = success;
	}

	public boolean isSuccess() {
		return m_success;
	}

}
