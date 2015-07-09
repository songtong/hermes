package com.ctrip.hermes.portal.console.dashboard;

public enum JspFile {
	TOPIC("/jsp/console/dash-topic.jsp"), //
	TOPIC_DETAIL("/jsp/console/dash-topic-detail.jsp"), //
	CLIENT("/jsp/console/dash-client.jsp"), //
	CLIENT_DETAIL("/jsp/console/dash-client-detail.jsp"), //
	BROKER("/jsp/console/dash-broker.jsp"), //
	BROKER_DETAIL("/jsp/console/dash-broker-detail.jsp"), //
	BROKER_DETAIL_HOME("/jsp/console/dash-broker-detail-home.jsp"); //

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
