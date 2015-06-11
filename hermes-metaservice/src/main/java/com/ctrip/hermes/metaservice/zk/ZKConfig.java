package com.ctrip.hermes.metaservice.zk;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ZKConfig.class)
public class ZKConfig {

	public int getZkConnectionTimeoutMillis() {
		return 3000;
	}

	public String getZkConnectionString() {
		return "127.0.0.1:2181";
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
