package com.ctrip.hermes.portal.config;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.hermes.Hermes.Env;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

@Named(type = PortalConfig.class)
public class PortalConfig implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(PortalConfig.class);

	@Inject
	private ClientEnvironment m_env;

	private Config m_configs = ConfigService.getAppConfig();

	private static final String KIBANA_URL_KEY = "portal.kibana.url";

	private static final String DOC_TYPE_KEY = "portal.elastic.document.type";

	private static final String ES_NAME_KEY = "portal.elastic.cluster.name";

	private static final String ES_CLUSTER_KEY = "portal.elastic.cluster";

	private String m_kibanaUrl;

	private String m_esDocType;

	private String m_esClusterName;

	private String m_esClusterString;

	private String getConfigFromApollo(String key, String defaultValue) {
		if (m_configs != null) {
			try {
				return m_configs.getProperty(key, defaultValue);
			} catch (Exception e) {
				log.error("Get config [{}] from apollo failed.", key, e);
			}
		}
		return defaultValue;
	}

	public String getKibanaBaseUrl() {
		return m_kibanaUrl;
	}

	public String getSyncHost() {
		String host = m_env.getGlobalConfig().getProperty("portal.sync.host");
		if (StringUtils.isBlank(host) && Env.LOCAL.equals(m_env.getEnv())) {
			return "127.0.0.1";
		}
		return host;
	}

	public String getPortalUatHost() {
		return m_env.getGlobalConfig().getProperty("portal.uat.host", "hermes.fx.uat.qa.nt.ctripcorp.com");
	}

	public String getPortalProdHost() {
		return m_env.getGlobalConfig().getProperty("portal.prod.host", "hermes.fx.ctripcorp.com");
	}

	public Pair<String, String> getAccount() {
		String username = m_env.getGlobalConfig().getProperty("portal.account.username", "hermes");
		String password = m_env.getGlobalConfig().getProperty("portal.account.password", "hermes123");
		return new Pair<String, String>(username, password);
	}

	public String getPortalFwsHost() {
		return m_env.getGlobalConfig().getProperty("portal.fws.host", "hermes.fws.qa.nt.ctripcorp.com");
	}

	public String getPortalToolsHost() {
		return m_env.getGlobalConfig().getProperty("portal.tools.host", "hermes.fx.tools.ctripcorp.com:8080");
	}

	public String getHermesEmailGroupAddress() {
		return m_env.getGlobalConfig().getProperty("hermes.emailgroup.address", "Rdkjmes@Ctrip.com");
	}

	public String getEmailTemplateDir() {
		return m_env.getGlobalConfig().getProperty("hermes.emailtemplates.dir", "/templates");
	}

	public List<Pair<String, Integer>> getElasticClusterNodes() {
		String defaultHost = "localhost";
		Integer defaultPort = 9300;
		if (Env.LOCAL.equals(m_env.getEnv())) {
			return parseEndpoints(defaultHost, defaultPort);
		}
		List<Pair<String, Integer>> l = parseEndpoints(m_esClusterString, defaultPort);
		return l.size() > 0 ? l : parseEndpoints(defaultHost, defaultPort);
	}

	public String getElasticClusterName() {
		return m_esClusterName;
	}

	public String getElasticDocumentType() {
		return m_esDocType;
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

	@Override
	public void initialize() throws InitializationException {
		if (Env.LOCAL.equals(m_env.getEnv())) {
			m_kibanaUrl = "http://localhost:5601";
		} else {
			m_kibanaUrl = getConfigFromApollo(KIBANA_URL_KEY, m_env.getGlobalConfig().getProperty(KIBANA_URL_KEY));
		}

		m_esDocType = getConfigFromApollo(DOC_TYPE_KEY, m_env.getGlobalConfig().getProperty(DOC_TYPE_KEY, "biz"));
		m_esClusterName = //
		getConfigFromApollo(ES_NAME_KEY, m_env.getGlobalConfig().getProperty(ES_NAME_KEY, "elasticsearch"));
		m_esClusterString = getConfigFromApollo(ES_CLUSTER_KEY, m_env.getGlobalConfig().getProperty(ES_CLUSTER_KEY));
	}
}
