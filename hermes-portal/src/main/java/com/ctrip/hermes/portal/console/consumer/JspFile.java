package com.ctrip.hermes.portal.console.consumer;

public enum JspFile {
	VIEW("/jsp/console/consumer/consumer.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
