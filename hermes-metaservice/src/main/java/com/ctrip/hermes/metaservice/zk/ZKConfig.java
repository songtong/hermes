package com.ctrip.hermes.metaservice.zk;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ZKConfig.class)
public class ZKConfig {

	ClientEnvironment env = PlexusComponentLocator.lookup(ClientEnvironment.class);

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

	public int getZkRetryBaseSleepTimeMillis() {
		return 1000;
	}

	public int getZkRetryMaxRetries() {
		return 3;
	}

	public int getZkSessionTimeoutMillis() {
		return 15 * 1000;
	}
}
