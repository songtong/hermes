package com.ctrip.hermes.consumer.engine.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerConfig.class)
public class ConsumerConfig {

	public int getLocalCacheSize() {
		return 50;
	}

	public long getRenewLeaseTimeMillisBeforeExpired() {
		return 2 * 1000L;
	}

	public long getStopConsumerTimeMillsBeforLeaseExpired() {
		return 50L;
	}

	public long getDefaultLeaseAcquireDelay() {
		return 500L;
	}

	public long getDefaultLeaseRenewDelay() {
		return 500L;
	}

	public int getPullMessagesThreshold() {
		return getLocalCacheSize() * 3 / 10; // 30% remains
	}

	public long getNoMessageWaitInterval() {
		return 50L;
	}

	public long getNoEndpointWaitInterval() {
		return 500L;
	}

	public int getNotifierThreadCount() {
		return 10;
	}

}
