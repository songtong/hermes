package com.ctrip.hermes.metaserver.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaServerConfig.class)
public class MetaServerConfig {

	public long getDefaultLeaseAcquireOrRenewRetryDelayMillis() {
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

	public long getBrokerHeartbeatTimeoutMillis() {
		return getBrokerLeaseTimeMillis() + 5000L;
	}

	public long getActiveBrokerCheckIntervalTimeMillis() {
		return 1000L;
	}

	public long getBrokerLeaseTimeMillis() {
		return 30 * 1000L;
	}

	public long getBrokerLeaseClientSideAdjustmentTimeMills() {
		return -3 * 1000L;
	}

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

}
