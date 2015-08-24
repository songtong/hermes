package com.ctrip.hermes.broker.zk;

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
	ClientEnvironment env;

	public int getZkConnectionTimeoutMillis() {
		return 3000;
	}

	public String getZkConnectionString() {
		return env.getGlobalConfig().getProperty("meta.zk.connectionString");
	}

	public int getZkCloseWaitMillis() {
		return 1000;
	}

	public String getZkNamespace() {
		return "hermes";
	}

	public int getSleepMsBetweenRetries() {
		return 1000;
	}

	public int getZkRetries() {
		return Integer.MAX_VALUE;
	}

	public int getZkSessionTimeoutMillis() {
		return 5 * 1000;
	}
}
