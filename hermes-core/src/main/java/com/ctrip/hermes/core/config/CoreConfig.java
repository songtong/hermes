package com.ctrip.hermes.core.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = CoreConfig.class)
public class CoreConfig {

	public int getCommandProcessorThreadCount() {
		return 10;
	}

	public int getMetaServerIpFetchInterval() {
		return 60;
	}

	public int getMetaServerConnectTimeout() {
		return 2000;
	}

	public int getMetaServerReadTimeout() {
		return 5000;
	}
}
