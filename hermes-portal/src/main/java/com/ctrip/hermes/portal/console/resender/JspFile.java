package com.ctrip.hermes.portal.console.resender;

public enum JspFile {
	VIEW("/jsp/console/resender.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
