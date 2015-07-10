package com.ctrip.hermes.portal.config;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.Hermes.Env;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

@Named(type = PortalConfig.class)
public class PortalConfig {
	private static final Logger log = LoggerFactory.getLogger(PortalConfig.class);

	@Inject
	ClientEnvironment m_env;

	public String getKibanaBaseUrl() {
		if (Env.LOCAL.equals(m_env.getEnv())) {
			return "http://localhost:5601";
		}
		return m_env.getGlobalConfig().getProperty("portal.kibana.url");
	}

	public List<Pair<String, Integer>> getElasticClusterNodes() {
		String defaultHost = "localhost";
		Integer defaultPort = 9300;
		if (Env.LOCAL.equals(m_env.getEnv())) {
			return parseEndpoints(defaultHost, defaultPort);
		}
		String propValue = m_env.getGlobalConfig().getProperty("portal.elastic.cluster");
		List<Pair<String, Integer>> l = parseEndpoints(propValue, defaultPort);
		return l.size() > 0 ? l : parseEndpoints(defaultHost, defaultPort);
	}

	private List<Pair<String, Integer>> parseEndpoints(String str, int defaultPort) {
		List<Pair<String, Integer>> l = new ArrayList<Pair<String, Integer>>();
		if (!StringUtils.isBlank(str)) {
			try {
				for (String host : str.split(",")) {
					String[] parts = host.split(":");
					l.add(new Pair<String, Integer>(parts[0], parts.length > 1 ? Integer.valueOf(parts[1]) : defaultPort));
				}
			} catch (Exception e) {
				log.error("Parse endpoints failed.", e);
			}
		}
		return l;
	}
}
