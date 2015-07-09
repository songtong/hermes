package com.ctrip.hermes.portal.console.dashboard;

public enum JspFile {
	TOPIC("/jsp/console/dash-topic.jsp"), //
	TOPIC_DETAIL("/jsp/console/dash-topic-detail.jsp"), //
	CLIENT("/jsp/console/dash-client.jsp"), //
	BROKER("/jsp/console/dash-broker.jsp");

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
