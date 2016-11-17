package com.ctrip.hermes.admin.core.service.mail;

import java.util.Arrays;
import java.util.List;

public class HermesMail {

	private String m_subject;

	private String m_body;

	private List<String> m_receivers;

	public HermesMail(String subject, String body, String receivers) {
		this(subject, body, Arrays.asList(receivers.split(",")));
	}

	public HermesMail(String subject, String body, List<String> receivers) {
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

	public List<String> getReceivers() {
		return m_receivers;
	}

	public void setReceivers(String receivers) {
		m_receivers = Arrays.asList(receivers.split(","));
	}

	public void setReceivers(List<String> receivers) {
		m_receivers = receivers;
	}

	@Override
	public String toString() {
		return "HermesMail [m_subject=" + m_subject + ", m_body=" + m_body + ", m_receivers=" + m_receivers + "]";
	}
}
