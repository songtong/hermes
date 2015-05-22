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

	public long getRunningStatusStatInterval() {
		return 30;
	}

	public long getMetaCacheRefreshIntervalMinutes() {
		return 1;
	}

	public long getSendMessageReadResultTimeout() {
		return 10 * 1000L;
	}

	public long getChannelPendingCmdsHouseKeepingInterval() {
		return 3;
	}

	public long getChannelWriteFailSleepTime() {
		return 50L;
	}

	public int getChannelAutoReconnectDelay() {
		return 1;
	}
}
