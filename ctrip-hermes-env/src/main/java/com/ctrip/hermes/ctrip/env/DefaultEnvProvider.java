package com.ctrip.hermes.ctrip.env;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

@Named(type = EnvProvider.class)
public class DefaultEnvProvider implements EnvProvider {

	private static final Logger log = LoggerFactory.getLogger(DefaultEnvProvider.class);

	private Env m_env;

	private String m_metaServerDomainName;

	private Map<Env, String> m_env2MetaDomain = new HashMap<>();

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

		String strEnv = config.getProperty("env");

		if (strEnv != null && strEnv.trim().length() > 0) {
			try {
				m_env = Env.valueOf(strEnv.toUpperCase());
			} catch (RuntimeException e) {
				throw new IllegalArgumentException(String.format(
				      "%s is not a valid hermes env, valid values are (dev, fws, uat, lpt, prod, tools)", strEnv));
			}
		} else {
			throw new IllegalArgumentException("Hermes env not set");
		}

		m_metaServerDomainName = m_env2MetaDomain.get(m_env);

		log.info("Hermes env is {}, meta-server domain is {}", m_env, m_metaServerDomainName);
	}

	@Override
	public String getMetaServerDomainName() {
		return m_metaServerDomainName;
	}

}
