package com.ctrip.hermes.portal.console.dashboard;

public enum JspFile {
	VIEW("/jsp/console/dashboard.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
