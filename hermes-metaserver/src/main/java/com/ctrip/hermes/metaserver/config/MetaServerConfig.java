package com.ctrip.hermes.metaserver.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaServerConfig.class)
public class MetaServerConfig {

	public long getDefaultLeaseAcquireOrRenewRetryDelayMills() {
		return 1000L;
	}

	public long getConsumerLeaseTimeMillis() {
		return 20 * 1000L;
	}

	public long getConsumerLeaseClientSideAdjustmentTimeMills() {
		return -2 * 1000L;
	}

	public long getActiveConsumerCheckIntervalTimeMillis() {
		return 1000L;
	}

	public long getConsumerHeartbeatTimeoutMillis() {
		return getConsumerLeaseTimeMillis() + 3000L;
	}

}
