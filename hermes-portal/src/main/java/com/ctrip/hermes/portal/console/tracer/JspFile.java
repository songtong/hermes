package com.ctrip.hermes.portal.console.tracer;

public enum JspFile {
	VIEW("/jsp/console/tracer.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
