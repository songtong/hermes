package com.ctrip.hermes.consumer.engine.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerConfig.class)
public class ConsumerConfig {

	public String getDefautlLocalCacheSize() {
		return "200";
	}

	public long getRenewLeaseTimeMillisBeforeExpired() {
		return 5 * 1000L;
	}

	public long getStopConsumerTimeMillsBeforLeaseExpired() {
		return getRenewLeaseTimeMillisBeforeExpired() - 3 * 1000L;
	}

	public long getDefaultLeaseAcquireDelayMillis() {
		return 500L;
	}

	public long getDefaultLeaseRenewDelayMillis() {
		return 500L;
	}

	public String getDefaultLocalCachePrefetchThresholdPercentage() {
		return "30";
	}

	public long getNoMessageWaitIntervalMillis() {
		return 50L;
	}

	public long getNoEndpointWaitIntervalMillis() {
		return 500L;
	}

	public String getDefaultNotifierThreadCount() {
		return "1";
	}

}
