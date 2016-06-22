package com.ctrip.hermes.core.bo;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageResult {
	private boolean m_success;

	private boolean m_shouldSkip;

	private String m_errorMessage;

	public SendMessageResult(boolean success, boolean shouldSkip, String errorMessage) {
		super();
		m_success = success;
		m_shouldSkip = shouldSkip;
		m_errorMessage = errorMessage;
	}

	public boolean isSuccess() {
		return m_success;
	}

	public boolean isShouldSkip() {
		return m_shouldSkip;
	}

	public String getErrorMessage() {
		return m_errorMessage;
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public void setShouldSkip(boolean shouldSkip) {
		m_shouldSkip = shouldSkip;
	}

	public void setErrorMessage(String errorMessage) {
		m_errorMessage = errorMessage;
	}

}
