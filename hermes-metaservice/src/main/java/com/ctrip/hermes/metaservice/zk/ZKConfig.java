package com.ctrip.hermes.metaservice.zk;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ZKConfig.class)
public class ZKConfig {

	@Inject
	private ClientEnvironment m_env;

	public void setEnv(ClientEnvironment env) {
		m_env = env;
	}

	public int getZkConnectionTimeoutMillis() {
		return 1000;
	}

	public String getZkConnectionString() {
		return m_env.getGlobalConfig().getProperty("meta.zk.connectionString");
	}

	public int getZkCloseWaitMillis() {
		return 1000;
	}

	public String getZkNamespace() {
		return "hermes";
	}

	public int getSleepMsBetweenRetries() {
		return 100;
	}

	public int getZkRetries() {
		return 3;
	}

	public int getZkSessionTimeoutMillis() {
		return 5 * 1000;
	}
}
