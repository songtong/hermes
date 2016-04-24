package com.ctrip.hermes.metaservice.assist;

import java.util.Map;

import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

public class HermesMailContext {
	private String m_title;

	private HermesTemplate m_hermesTemplate;

	private Map<String, Object> m_contentMap;

	public HermesMailContext(String title, HermesTemplate template, Map<String, Object> contentMap) {
		m_title = title;
		m_hermesTemplate = template;
		m_contentMap = contentMap;
	}

	public String getTitle() {
		return m_title;
	}

	public HermesTemplate getHermesTemplate() {
		return m_hermesTemplate;
	}

	public Map<String, Object> getContentMap() {
		return m_contentMap;
	}
}
