package com.ctrip.hermes.portal.console.home;

public enum JspFile {
	VIEW("/jsp/console/home/home.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
