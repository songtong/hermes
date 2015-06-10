package com.ctrip.hermes.portal.console.storage;

public enum JspFile {
	VIEW("/jsp/console/storage.jsp"),

	;

	private String m_path;

	private JspFile(String path) {
		m_path = path;
	}

	public String getPath() {
		return m_path;
	}
}
