package com.ctrip.hermes.consumer.engine.config;

import java.io.IOException;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerConfig.class)
public class ConsumerConfig {

	public static final int DEFAULT_LOCALCACHE_SIZE = 10;

	@Inject
	private ClientEnvironment m_clientEnv;

	public int getLocalCacheSize(String topic) throws IOException {
		String localCacheSizeStr = m_clientEnv.getConsumerConfig(topic).getProperty("consumer.localcache.size");

		if (StringUtils.isNumeric(localCacheSizeStr)) {
			return Integer.valueOf(localCacheSizeStr);
		}

		return DEFAULT_LOCALCACHE_SIZE;
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

	public int getNoMessageWaitBaseMillis() {
		return 50;
	}

	public int getNoMessageWaitMaxMillis() {
		return 500;
	}

	public int getNoEndpointWaitBaseMillis() {
		return 500;
	}

	public int getNoEndpointWaitMaxMillis() {
		return 4000;
	}

	public String getDefaultNotifierThreadCount() {
		return "1";
	}

	public long getPullMessageBrokerExpireTimeAdjustmentMills() {
		return -500L;
	}

	public long getQueryOffsetTimeoutMillis() {
		return 3000;
	}

}
