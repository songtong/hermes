package com.ctrip.hermes.portal.console.template;

public enum JspFile {
	VIEW("/jsp/console/"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
