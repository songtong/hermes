package com.ctrip.hermes.monitor.checker;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.core.utils.StringUtils.StringFormatter;

public class CatUrlBuilder {
	private static final String PARAM_KEY_TYPE = "type";

	private static final String PARAM_KEY_NAME = "name";

	private static final String PARAM_KEY_DOMAIN = "domain";

	private static final String PARAM_KEY_IP = "ip";

	private static final String PARAM_KEY_DATE = "date";

	private String m_base;

	private String m_path;

	Map<String, String> m_params = new HashMap<>();

	public CatUrlBuilder(String base, boolean txOrEvent) {
		m_base = base;
		m_path = txOrEvent ? "/cat/r/t?op=graphs&" : "/cat/r/e?op=graphs&";
	}

	public CatUrlBuilder domain(String domain) {
		m_params.put(PARAM_KEY_DOMAIN, domain);
		return this;
	}

	public CatUrlBuilder ip(String ip) {
		m_params.put(PARAM_KEY_IP, ip);
		return this;
	}

	public CatUrlBuilder type(String type) {
		m_params.put(PARAM_KEY_TYPE, type);
		return this;
	}

	public CatUrlBuilder name(String name) {
		m_params.put(PARAM_KEY_NAME, name);
		return this;
	}

	public CatUrlBuilder date(String date) {
		m_params.put(PARAM_KEY_DATE, date);
		return this;
	}

	public CatUrlBuilder xml() {
		m_params.put("forceDownload", "xml");
		return this;
	}

	public String build() {
		if (StringUtils.isBlank(m_base) || StringUtils.isBlank(m_path)) {
			throw new RuntimeException("Must set url base and (tx || event)");
		}
		StringBuilder sb = new StringBuilder(m_base + m_path);
		sb.append(StringUtils.join(m_params.entrySet(), "&", new StringFormatter<Entry<String, String>>() {
			@Override
			public String format(Entry<String, String> obj) {
				return obj.getKey() + "=" + obj.getValue();
			}
		}));
		return sb.toString();
	}

	public String getBase() {
		return m_base;
	}

	public String getPath() {
		return m_path;
	}

	public String getType() {
		return m_params.get(PARAM_KEY_TYPE);
	}

	public String getName() {
		return m_params.get(PARAM_KEY_NAME);
	}

	public String getDomain() {
		return m_params.get(PARAM_KEY_DOMAIN);
	}

	public String getIp() {
		return m_params.get(PARAM_KEY_IP);
	}

	public String getDateString() {
		return m_params.get(PARAM_KEY_DATE);
	}
}
