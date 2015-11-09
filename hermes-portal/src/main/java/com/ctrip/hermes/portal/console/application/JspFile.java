package com.ctrip.hermes.portal.console.application;

public enum JspFile {
	VIEW("/jsp/console/application/application.jsp"),
	TOPIC("/jsp/console/application/application-topic.jsp"),
	CONSUMER("/jsp/console/application/application-consumer.jsp"),
	REVIEW("/jsp/console/application/application-review.jsp"),
	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
