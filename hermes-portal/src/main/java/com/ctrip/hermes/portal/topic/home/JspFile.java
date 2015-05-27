package com.ctrip.hermes.portal.topic.home;

public enum JspFile {
	VIEW("/jsp/topic/home.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
