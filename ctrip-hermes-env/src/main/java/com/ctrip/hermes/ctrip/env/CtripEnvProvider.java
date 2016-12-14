package com.ctrip.hermes.ctrip.env;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.framework.foundation.Foundation;

@Named(type = EnvProvider.class)
public class CtripEnvProvider implements EnvProvider {

	private static final Logger log = LoggerFactory.getLogger(CtripEnvProvider.class);

	private Env m_env;

	private String m_metaServerDomainName;

	private Map<Env, String> m_env2MetaDomain = new HashMap<>();

	private String m_idc;

	@Override
	public String getEnv() {
		return m_env.toString();
	}

	public void initialize(Properties config) {
		m_env2MetaDomain.put(Env.LOCAL, config.getProperty("local.domain", "meta.hermes.local"));
		m_env2MetaDomain.put(Env.DEV, config.getProperty("dev.domain", "10.3.8.63"));
		m_env2MetaDomain.put(Env.LPT, config.getProperty("lpt.domain", "10.2.5.133"));
		m_env2MetaDomain.put(Env.FAT, config.getProperty("fat.domain", "meta.hermes.fws.qa.nt.ctripcorp.com"));
		m_env2MetaDomain.put(Env.FWS, config.getProperty("fws.domain", "meta.hermes.fws.qa.nt.ctripcorp.com"));
		m_env2MetaDomain.put(Env.UAT, config.getProperty("uat.domain", "meta.hermes.fx.uat.qa.nt.ctripcorp.com"));
		m_env2MetaDomain.put(Env.PROD, config.getProperty("prod.domain", "meta.hermes.fx.ctripcorp.com"));
		m_env2MetaDomain.put(Env.TOOLS, config.getProperty("tools.domain", "meta.hermes.fx.tools.ctripcorp.com"));

		String strEnvFromConfig = config.getProperty("env");
		String strEnvFromFoundation = Foundation.server().getEnvType();

		refineEnv(strEnvFromConfig, strEnvFromFoundation, Foundation.server().isTooling());

		refineIdc();

		m_metaServerDomainName = m_env2MetaDomain.get(m_env);

		log.info("Hermes env is {}, meta-server domain is {}, idc is {}", m_env, m_metaServerDomainName, m_idc);
	}

	private void refineIdc() {
		String idc = Foundation.server().getDataCenter();
		if (idc != null && idc.trim().length() > 0) {
			switch (idc.toUpperCase()) {
			case "SHAFQ":
				m_idc = "FQ";
				break;
			case "SHAJQ":
				m_idc = "JQ";
				break;
			case "SHAOY":
				m_idc = "OY";
				break;
			case "NTGXH":
				m_idc = "NT";
				break;
			default:
				log.warn("Unknown idc value {} from framework-foundation", idc);
				break;
			}
		} else {
			log.warn("No idc found from framework-foundation");
		}
	}

	private void refineEnv(String strEnvFromConfig, String strEnvFromFoundation, boolean tooling) {
		Env envFromConfig = null;
		Env envFromFoundation = null;

		if (strEnvFromConfig != null && strEnvFromConfig.trim().length() > 0) {
			envFromConfig = toEnv(strEnvFromConfig, false);

			if (envFromConfig == null) {
				throw new IllegalArgumentException(
				      String.format(
				            "%s is not a valid Hermes env(from hermes.properties), valid values are (dev, fws, uat, lpt, prod, prd, pro, tools)",
				            strEnvFromConfig));
			} else {
				log.info("Hermes env from hermes.properties is {}", envFromConfig);
			}
		}

		if (strEnvFromFoundation != null && strEnvFromFoundation.trim().length() > 0) {
			log.info("Hermes env from framework-foundation is {}, tooling is {}", strEnvFromFoundation, tooling);
			envFromFoundation = toEnv(strEnvFromFoundation, tooling);
		} else {
			log.warn("No env found from framework-foundation");
		}

		if (envFromConfig != null && envFromFoundation != null) {
			if (!envFromConfig.equals(envFromFoundation)) {
				log.warn("Hermes env from hermes.properties({}) conflict with env from foundation({}), will use {} as env",
				      envFromConfig, envFromFoundation, envFromConfig);
			}
			m_env = envFromConfig;
		} else {
			m_env = envFromConfig == null ? envFromFoundation : envFromConfig;
		}

		if (m_env == null) {
			throw new IllegalArgumentException("Hermes env not set");
		}
	}

	private Env toEnv(String envStr, boolean isTooling) {
		switch (envStr.toUpperCase()) {
		case "LOCAL":
			return Env.LOCAL;
		case "DEV":
			return Env.DEV;
		case "FAT":
			return Env.FAT;
		case "FWS":
			return Env.FWS;
		case "LPT":
			return Env.LPT;
		case "PRO":
		case "PROD":
		case "PRD":
			if (isTooling) {
				return Env.TOOLS;
			} else {
				return Env.PROD;
			}
		case "TOOLS":
			return Env.TOOLS;
		case "UAT":
			return Env.UAT;
		default:
			return null;
		}
	}

	@Override
	public String getMetaServerDomainName() {
		return m_metaServerDomainName;
	}

	@Override
	public String getIdc() {
		return m_idc;
	}

}
