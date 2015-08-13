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

	public String getSyncHost() {
		String host = m_env.getGlobalConfig().getProperty("portal.sync.host");
		if (StringUtils.isBlank(host) && Env.LOCAL.equals(m_env.getEnv())) {
			return "127.0.0.1";
		}
		return host;
	}

	public Pair<String, String> getAccount() {
		String username = m_env.getGlobalConfig().getProperty("portal.account.username", "hermes");
		String password = m_env.getGlobalConfig().getProperty("portal.account.password", "hermes123");
		return new Pair<String, String>(username, password);
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

	public String getElasticClusterName() {
		return m_env.getGlobalConfig().getProperty("portal.elastic.cluster.name", "elasticsearch");
	}

	public String getElasticDocumentType() {
		return m_env.getGlobalConfig().getProperty("portal.elastic.document.type", "biz");
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
