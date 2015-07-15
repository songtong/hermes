package com.ctrip.hermes.portal.console.topic;

public enum JspFile {
	VIEW("/jsp/console/topic/topic.jsp"), DETAIL("/jsp/console/topic/topic-detail.jsp");

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
