package com.ctrip.hermes.portal.console.dashboard;

public enum JspFile {
	TOPIC("/jsp/console/dashboard/dash-topic.jsp"), //
	TOPIC_DETAIL("/jsp/console/dashboard/dash-topic-detail.jsp"), //
	CLIENT("/jsp/console/dashboard/dash-client.jsp"), //
	BROKER("/jsp/console/dashboard/dash-broker.jsp"), //
	BROKER_DETAIL("/jsp/console/dashboard/dash-broker-detail.jsp"), //
	BROKER_DETAIL_HOME("/jsp/console/dashboard/dash-broker-detail-home.jsp"), //
	MONITOR_EVENT("/jsp/console/dashboard/monitor-event.jsp"); //

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
