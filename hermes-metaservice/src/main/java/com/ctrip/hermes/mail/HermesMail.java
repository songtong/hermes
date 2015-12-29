package com.ctrip.hermes.mail;

public class HermesMail {

	private String m_subject;

	private String m_body;

	private String m_receivers;

	public HermesMail(String subject, String body, String receivers) {
		m_subject = subject;
		m_body = body;
		m_receivers = receivers;
	}

	public String getSubject() {
		return m_subject;
	}

	public void setSubject(String subject) {
		m_subject = subject;
	}

	public String getBody() {
		return m_body;
	}

	public void setBody(String body) {
		m_body = body;
	}

	public String getReceivers() {
		return m_receivers;
	}

	public void setReceivers(String receivers) {
		m_receivers = receivers;
	}

}
